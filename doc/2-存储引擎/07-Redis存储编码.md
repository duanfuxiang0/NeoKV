# Redis 存储编码

## 本章概览

在这一章中，我们将了解：

- NeoKV 的两层编码系统：Phase 1（旧编码）与 Phase 2（新编码）
- Metadata CF 的编码格式：Flags、过期时间、版本号、元素数量
- Subkey CF 的编码格式：复合类型的子元素存储
- Score CF 的编码格式：ZSet 的分数索引
- Version-Based Lazy Deletion：大 key 删除的核心机制
- 过期机制：被动过期与主动清理的配合

这一章是 Part 2 的收尾，也是 Part 3（数据结构实现）的基础。理解了编码设计，后续每种数据类型的实现就水到渠成了。

**关键文件**：
- `include/redis/redis_metadata.h` — 所有编码类定义
- `src/redis/redis_metadata.cpp` — 编解码实现、Version 生成
- `include/redis/redis_codec.h` — Phase 1 旧编码
- `src/redis/redis_codec.cpp` — 旧编码实现

## 1. 两层编码系统

NeoKV 的 Redis 存储编码经历了两个阶段的演进：

### 1.1 Phase 1：简单编码（RedisCodec）

最初的实现只支持 String 类型，编码简单直接：

```
Key (DATA_CF):
┌──────────┬──────────┬──────┬────────────┬─────────┬──────────┐
│region_id │ index_id │ slot │user_key_len│ user_key│ type_tag │
│   8B     │   8B     │  2B  │    4B      │  变长   │    1B    │
└──────────┴──────────┴──────┴────────────┴─────────┴──────────┘

Value (DATA_CF):
┌──────────────┬─────────┐
│ expire_at_ms │ payload │
│     8B       │  变长   │
└──────────────┴─────────┘
```

`type_tag` 是一个 ASCII 字符，标识数据类型：

```cpp
// include/redis/redis_codec.h
enum RedisTypeTag : uint8_t {
    REDIS_STRING = 'S',   // 0x53
    REDIS_HASH   = 'H',   // 0x48
    REDIS_LIST   = 'L',   // 0x4C
    REDIS_SET    = 'E',   // 0x45
    REDIS_ZSET   = 'Z',   // 0x5A
};
```

这种编码的问题：
- **类型信息在 key 中**：查询一个 key 的类型需要先构造完整的 key
- **不支持复合类型的元数据**：Hash/Set/List/ZSet 需要存储 size、version 等信息，没有地方放
- **无法实现 Lazy Deletion**：删除大 key 必须逐条删除子元素

### 1.2 Phase 2：Metadata 编码（RedisMetadata）

为了支持复合类型，我们从 Kvrocks 移植了 Metadata 编码方案。核心思路是**分离元数据和子键数据**：

```
REDIS_METADATA_CF:  存储每个 key 的元信息（类型、过期、版本、大小）
DATA_CF:            存储复合类型的子元素（Hash field、Set member 等）
REDIS_ZSET_SCORE_CF: 存储 ZSet 的分数索引
```

### 1.3 迁移策略

两种编码共存，通过以下策略平滑迁移：

```
写入：始终写入 Metadata CF（新编码）
      如果是 String 类型，同时删除 Data CF 中的旧编码 key

读取：先查 Metadata CF
      如果未找到，回退查 Data CF（旧编码）
      如果在旧编码中找到，下次写入时自动迁移到新编码

FlushDB：同时清理两个 CF
```

这种"读时回退 + 写时迁移"的策略让我们不需要一次性迁移所有数据，旧数据会随着正常的读写操作逐渐迁移到新编码。

## 2. Metadata CF 编码

### 2.1 Metadata Key

```
┌──────────┬──────────┬──────┬────────────┬─────────┐
│region_id │ index_id │ slot │user_key_len│ user_key│
│   8B     │   8B     │  2B  │    4B      │  变长   │
└──────────┴──────────┴──────┴────────────┴─────────┘
```

与 Phase 1 的 key 相比，**去掉了末尾的 type_tag**。类型信息存储在 value 的 flags 字节中。

```cpp
// include/redis/redis_metadata.h — RedisMetadataKey
class RedisMetadataKey {
    int64_t _region_id;
    int64_t _index_id;
    uint16_t _slot;
    std::string _user_key;

    std::string encode() const;           // 编码为 RocksDB key
    void decode(const rocksdb::Slice& s); // 从 RocksDB key 解码
};
```

### 2.2 Metadata Value — Flags 字节

每个 Metadata value 的第一个字节是 flags：

```
Flags 字节:
┌───┬───────────┬──────────────┐
│ 1 │  000      │    type      │
│bit│  3-bit    │   4-bit      │
│64b│ reserved  │  RedisType   │
└───┴───────────┴──────────────┘
 MSB                         LSB
```

- **Bit 7**（`METADATA_64BIT_ENCODING_MASK = 0x80`）：始终为 1，标识 64 位编码
- **Bit 4-6**：保留
- **Bit 0-3**（`METADATA_TYPE_MASK = 0x0F`）：Redis 类型枚举

```cpp
// include/redis/redis_metadata.h
enum RedisType : uint8_t {
    kRedisNone   = 0,
    kRedisString = 1,
    kRedisHash   = 2,
    kRedisList   = 3,
    kRedisSet    = 4,
    kRedisZSet   = 5,
};
```

### 2.3 String 类型的 Value（内联）

String 是**单 KV 类型**（`IsSingleKVType = true`），值直接内联在 Metadata value 中：

```
┌───────┬──────────────┬───────────────┐
│ flags │  expire_ms   │ inline_value  │
│  1B   │     8B       │    变长       │
└───────┴──────────────┴───────────────┘
```

GET/SET 只需一次 RocksDB 读写，不需要查询子键。这是 String 类型性能最优的关键。

### 2.4 复合类型的 Value（Hash/Set/ZSet）

```
┌───────┬──────────────┬──────────┬──────┐
│ flags │  expire_ms   │ version  │ size │
│  1B   │     8B       │   8B     │  8B  │
└───────┴──────────────┴──────────┴──────┘
```

- `expire_ms`：绝对过期时间戳（毫秒），0 表示永不过期
- `version`：64 位版本号，用于 Lazy Deletion（见第 5 节）
- `size`：元素数量（Hash 的 field 数、Set 的 member 数等）

### 2.5 List 类型的 Value（额外字段）

List 在复合类型的基础上增加了 head/tail 指针：

```
┌───────┬──────────────┬──────────┬──────┬──────┬──────┐
│ flags │  expire_ms   │ version  │ size │ head │ tail │
│  1B   │     8B       │   8B     │  8B  │  8B  │  8B  │
└───────┴──────────────┴──────────┴──────┴──────┴──────┘
```

`head` 和 `tail` 是 uint64 指针，初始值为 `UINT64_MAX / 2`：

```cpp
// src/redis/redis_metadata.cpp
RedisListMetadata::RedisListMetadata(bool generate_version)
    : RedisMetadata(kRedisList, generate_version),
      head(UINT64_MAX / 2), tail(UINT64_MAX / 2) {}
```

从中间开始，允许向两端增长。LPUSH 递减 head，RPUSH 递增 tail。这个设计让 List 的两端操作都是 O(1)。

### 2.6 编解码实现

```cpp
// src/redis/redis_metadata.cpp — 编码
std::string RedisMetadata::encode() const {
    std::string dst;
    uint8_t flags = METADATA_64BIT_ENCODING_MASK | (type & METADATA_TYPE_MASK);
    dst.append(1, flags);
    PutFixed64BigEndian(&dst, expire_ms);

    if (!is_single_kv_type()) {
        PutFixed64BigEndian(&dst, version);
        PutFixed64BigEndian(&dst, size);
    }
    return dst;
}

// 解码
void RedisMetadata::decode(const rocksdb::Slice& input) {
    uint8_t flags = input[0];
    type = static_cast<RedisType>(flags & METADATA_TYPE_MASK);
    expire_ms = DecodeFixed64BigEndian(input.data() + 1);

    if (!is_single_kv_type()) {
        version = DecodeFixed64BigEndian(input.data() + 9);
        size = DecodeFixed64BigEndian(input.data() + 17);
    }
}
```

所有多字节字段使用**大端编码**（Big-Endian），与 key 编码保持一致。

## 3. Subkey CF 编码（DATA_CF）

复合类型的子元素存储在 DATA_CF 中。

### 3.1 Subkey Key 格式

```
┌──────────────────────────────────┬──────────┬─────────┐
│        metadata key prefix       │ version  │ sub_key │
│  (region_id + index_id + slot    │   8B     │  变长   │
│   + user_key_len + user_key)     │          │         │
└──────────────────────────────────┴──────────┴─────────┘
```

前缀与 Metadata key 完全相同，后面追加 `version` 和 `sub_key`。

`version` 字段是 Lazy Deletion 的核心：只有 version 与 Metadata 中的 version 匹配的子键才是有效的。

`sub_key` 的含义因类型而异：

| 类型 | sub_key 内容 | 示例 |
|------|-------------|------|
| Hash | field name（原始字节） | `"name"`, `"age"` |
| Set | member（原始字节） | `"alice"`, `"bob"` |
| List | big-endian uint64 index | `0x8000000000000000`（初始 head） |
| ZSet | member（原始字节） | `"player1"`, `"player2"` |

### 3.2 Subkey Value 格式

| 类型 | value 内容 |
|------|-----------|
| Hash | field value（原始字节） |
| Set | 空（member 就是 sub_key 本身） |
| List | element value（原始字节） |
| ZSet | encoded_double(score)（8 字节） |

Set 的 value 为空是一个巧妙的设计——成员信息完全编码在 key 中，RocksDB 的 key 存在性检查就等价于成员存在性检查。

### 3.3 前缀迭代

遍历某个 key 的所有子元素时，构建前缀并使用 prefix seek：

```cpp
// include/redis/redis_metadata.h — RedisSubkeyKey
std::string RedisSubkeyKey::encode_prefix() const {
    // [metadata_key_prefix][version:8]
    // 用于迭代该 key + version 下的所有子键
}
```

例如，`HGETALL mykey` 的实现：

```cpp
std::string prefix = RedisSubkeyKey(region_id, index_id, slot,
                                     "mykey", metadata.version)
                     .encode_prefix();
auto* iter = db->NewIterator(read_options, data_cf);
for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
     iter->Next()) {
    // 解码 field name 和 value
}
```

## 4. Score CF 编码（REDIS_ZSET_SCORE_CF）

ZSet 需要支持两种查询模式：
- **按 member 查询**：`ZSCORE key member` → 在 DATA_CF 中查找
- **按 score 范围查询**：`ZRANGEBYSCORE key min max` → 在 SCORE_CF 中查找

因此 ZSet 使用**双 CF 索引**：

```
DATA_CF (member → score):
Key:   [prefix][version][member]
Value: [encoded_score:8]

REDIS_ZSET_SCORE_CF (score+member → 空):
Key:   [prefix][version][score:8][member]
Value: 空
```

### 4.1 Score 的字节序保持编码

IEEE 754 double 的字节表示不能直接用于字节序比较（负数、NaN 等情况）。NeoKV 使用一种编码让 double 的字节序与数值序一致：

```cpp
// src/redis/redis_metadata.cpp
void RedisZSetScoreKey::encode_score(double score, std::string* dst) {
    uint64_t bits;
    memcpy(&bits, &score, sizeof(bits));

    if (bits & (1ULL << 63)) {
        // 负数：全部取反
        bits = ~bits;
    } else {
        // 正数（含 +0）：翻转符号位
        bits ^= (1ULL << 63);
    }

    PutFixed64BigEndian(dst, bits);
}
```

编码后的 8 字节按字节比较的顺序与 double 的数值顺序完全一致：
- `-inf < -1.0 < -0.0 < +0.0 < 1.0 < +inf`

### 4.2 Score CF 的 Value

Score CF 的 value 为空——所有信息都编码在 key 中。这是因为 Score CF 只用于范围查询（迭代），不需要存储额外数据。member 和 score 都可以从 key 中解码出来。

## 5. Version-Based Lazy Deletion

这是从 Kvrocks 移植的核心机制，解决了一个关键的性能问题。

### 5.1 问题

假设一个 Hash key 有 100 万个 field。当执行 `DEL mykey` 时：

**朴素方案**：遍历并删除 100 万个子键 → 阻塞 Raft apply 线程数秒 → 所有写入被阻塞

这在生产环境中是不可接受的。Redis 自身也有类似问题，所以引入了 `UNLINK` 命令（异步删除）。

### 5.2 方案

Lazy Deletion 的核心思路：**删除操作只更新 Metadata，不触碰子键**。

```
删除前：
  Metadata CF: mykey → {type=Hash, version=100, size=1000000}
  Data CF:     mykey/v100/field1 → value1
               mykey/v100/field2 → value2
               ... (100 万条)

执行 DEL mykey：
  Metadata CF: mykey → 删除（或写入新 version）
  Data CF:     不做任何操作！

删除后：
  Metadata CF: mykey 不存在
  Data CF:     mykey/v100/field1 → value1  ← "孤儿"子键
               mykey/v100/field2 → value2  ← "孤儿"子键
               ... (100 万条孤儿)
```

DEL 操作变成了 O(1)——只需删除一条 Metadata 记录。

### 5.3 孤儿子键的处理

旧 version 的子键变成了"孤儿"，通过以下机制清理：

1. **读时过滤**：读取子键时，比较其 version 与 Metadata 的 version。不匹配则视为不存在。
2. **Compaction Filter**（规划中）：在 compaction 时检查子键的 version 是否仍然有效，无效则删除。
3. **自然覆盖**：如果同一个 key 被重新创建（比如再次 `HSET mykey ...`），新数据使用新 version，旧子键自然被隔离。

### 5.4 Version 生成算法

Version 必须全局唯一且单调递增：

```cpp
// src/redis/redis_metadata.cpp
static std::atomic<uint64_t> version_counter_;
static const int kVersionCounterBits = 11;

uint64_t RedisMetadata::generate_version() {
    int64_t timestamp = current_time_us();  // 微秒时间戳
    uint64_t counter = version_counter_.fetch_add(1);
    return (static_cast<uint64_t>(timestamp) << kVersionCounterBits)
         + (counter % (1 << kVersionCounterBits));
}
```

Version 格式：`[53-bit 微秒时间戳][11-bit 原子计数器]`

- 53 位时间戳提供粗粒度的单调性（微秒级）
- 11 位计数器（0-2047）保证同一微秒内的唯一性
- 计数器在启动时随机初始化，避免 Follower 提升为 Leader 后与旧 Leader 的 version 冲突

### 5.5 Lazy Deletion 的完整流程

以 `DEL mykey`（mykey 是一个 Hash）为例：

```
1. 读取 Metadata: mykey → {type=Hash, version=100, size=500}

2. 删除 Metadata: WriteBatch.Delete(metadata_cf, mykey)
   （不删除 Data CF 中的任何子键）

3. Raft apply 完成，返回客户端 "1"（删除了 1 个 key）

4. 后续如果有人 HGET mykey field1：
   - 查 Metadata CF → 不存在 → 返回 nil

5. 后续如果有人 HSET mykey field1 newvalue：
   - 创建新 Metadata: {type=Hash, version=200, size=1}
   - 写入子键: mykey/v200/field1 → newvalue
   - 旧子键 mykey/v100/* 仍然存在，但 version 不匹配，永远不会被读到

6. 最终，旧子键在 compaction 时被清理
```

## 6. 过期机制

NeoKV 的过期机制由三层组成：

### 6.1 被动过期（读时检查）

每次读取 key 时，检查 Metadata 中的 `expire_ms`：

```cpp
bool RedisMetadata::is_expired() const {
    if (expire_ms == 0) return false;  // 永不过期
    return current_time_ms() >= expire_ms;
}
```

如果已过期，视为不存在，返回 nil。这是最基本的过期保证。

### 6.2 主动清理（TTL Cleaner）

`RedisTTLCleaner` 后台线程定期扫描并删除过期 key：

```cpp
// src/redis/redis_ttl_cleaner.cpp — 简化逻辑
void RedisTTLCleaner::clean_expired_keys() {
    for (auto& region : leader_regions) {
        auto* iter = db->NewIterator(read_options, metadata_cf);
        int count = 0;

        for (iter->SeekToFirst(); iter->Valid() && count < 1000; iter->Next()) {
            RedisMetadata metadata;
            metadata.decode(iter->value());

            if (metadata.is_expired()) {
                // 删除 Metadata（子键通过 Lazy Deletion 处理）
                db->Delete(metadata_cf, iter->key());
                count++;
            }
        }
    }
}
```

关键设计：
- **只在 Leader Region 上执行**：避免 Leader/Follower 数据不一致
- **每 Region 每轮最多 1000 个**：避免长时间占用资源
- **默认 60 秒间隔**：可通过 `redis_ttl_cleanup_interval_s` 配置

### 6.3 Compaction Filter（规划中）

未来可以在 REDIS_METADATA_CF 上添加 Compaction Filter，在 compaction 时自动清理过期的 Metadata。这将进一步减少过期数据占用的空间。

## 7. 编码总览

```
┌─────────────────────────────────────────────────────────────────┐
│                     REDIS_METADATA_CF                            │
│                                                                  │
│  Key: [region_id][index_id][slot][user_key_len][user_key]       │
│                                                                  │
│  Value (String):  [flags][expire_ms][inline_value]              │
│  Value (Hash):    [flags][expire_ms][version][size]             │
│  Value (Set):     [flags][expire_ms][version][size]             │
│  Value (ZSet):    [flags][expire_ms][version][size]             │
│  Value (List):    [flags][expire_ms][version][size][head][tail] │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         DATA_CF                                  │
│                                                                  │
│  Subkey Key: [metadata_prefix][version][sub_key]                │
│                                                                  │
│  Hash:  sub_key = field_name,  value = field_value              │
│  Set:   sub_key = member,      value = 空                       │
│  List:  sub_key = index(u64),  value = element                  │
│  ZSet:  sub_key = member,      value = encoded_score            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    REDIS_ZSET_SCORE_CF                           │
│                                                                  │
│  Score Key: [metadata_prefix][version][score:8][member]         │
│  Value: 空                                                      │
└─────────────────────────────────────────────────────────────────┘
```

## 8. 代码导读

| 文件 | 内容 |
|------|------|
| `include/redis/redis_metadata.h` | RedisType 枚举、RedisMetadata / RedisListMetadata 类、RedisMetadataKey / RedisSubkeyKey / RedisZSetScoreKey 编码类 |
| `src/redis/redis_metadata.cpp` | 编解码实现、Version 生成、Score 编码 |
| `include/redis/redis_codec.h` | Phase 1 旧编码：RedisCodec、RedisTypeTag |
| `src/redis/redis_codec.cpp` | 旧编码的 encode/decode 实现 |
| `include/redis/redis_common.h` | REDIS_SLOT_COUNT (16384)、is_redis_table() |

## 检验你的理解

- 为什么 String 类型的值内联在 Metadata 中，而不像其他类型使用子键？如果一个 String 的值非常大（比如 10MB），这种设计还合适吗？
- Version-Based Lazy Deletion 的孤儿子键在 compaction 之前会一直占用磁盘空间。如果系统写入量很大、compaction 跟不上，会有什么影响？
- List 的 head/tail 初始值为 `UINT64_MAX / 2`。如果一个 List 只做 LPUSH（head 不断递减），理论上最多能存多少个元素？
- Score 编码中，负数需要"全部取反"而正数只需"翻转符号位"，为什么？（提示：考虑 IEEE 754 的二进制表示）
- 两层编码系统的迁移策略是"读时回退 + 写时迁移"。如果有一个 key 从来不被写入，只被读取，它会永远留在旧编码中吗？这有什么问题？

---

> Part 2 到此结束。我们已经完整了解了 NeoKV 的存储层：从 RocksDB 的核心特性，到 8 个 CF 的设计，再到 Redis 数据类型的编码方案。
>
> 接下来进入 [Part 3: Redis 数据结构](../part3-Redis数据结构/08-String类型.md)，我们将逐一了解五种数据类型的具体实现。
