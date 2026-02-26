# Redis 存储编码

## 概览

- NeoKV 的两层编码系统：Phase 1（旧编码）与 Phase 2（新编码）
- Metadata CF 的编码格式：Flags、过期时间、版本号、元素数量
- Subkey CF 的编码格式：复合类型的子元素存储
- Score CF 的编码格式：ZSet 的分数索引
- Version-Based Lazy Deletion：大 key 删除的核心机制
- 过期机制：被动过期与主动清理的配合

这一章是 Part 2 的收尾，也是 Part 3（数据结构实现）的基础。理解了编码设计，后续每种数据类型的实现就水到渠成了。

## 一个编码系统的演进

NeoKV 的 Redis 存储编码经历了两个阶段。理解这段演进历史，有助于你理解代码中为什么存在两套编码逻辑，以及它们如何共存。

**Phase 1 是最初的简单编码**（`include/redis/redis_codec.h`）。那时我们只支持 String 类型，编码很直接：key 是标准的 16 字节前缀（`region_id` + `index_id`）加上 slot、user_key，末尾追加一个 ASCII 字符作为类型标记——`'S'` 表示 String，`'H'` 表示 Hash，`'L'` 表示 List，`'E'` 表示 Set（取 sEt 的 E），`'Z'` 表示 ZSet。value 则是 8 字节的过期时间戳加上实际数据。所有数据存在 DATA_CF 中。

这种编码对于 String 来说完全够用，但当我们要支持 Hash、Set、List、ZSet 这些复合类型时，问题就来了。一个 Hash key 可能有成千上万个 field，你需要在某个地方记录"这个 Hash 有多少个 field"、"它的版本号是多少"——但 Phase 1 的编码没有给这些元数据留位置。更关键的是，Phase 1 无法支持 Lazy Deletion——删除一个有 100 万个 field 的 Hash，你必须逐条删除所有子元素，没有别的办法。

**Phase 2 引入了 Metadata 编码**（`include/redis/redis_metadata.h`），核心思路是将元数据和子键数据分离到不同的 CF 中。REDIS_METADATA_CF 存储每个 key 的元信息（类型、过期时间、版本号、大小），DATA_CF 存储复合类型的子元素（Hash field、Set member 等），REDIS_ZSET_SCORE_CF 存储 ZSet 的分数索引。这个方案参考了 [Kvrocks](https://github.com/apache/kvrocks) 的编码设计。

两种编码通过一个平滑的迁移策略共存：写入时始终使用新编码（Metadata CF），如果是 String 类型还会同时删除 DATA_CF 中可能存在的旧编码 key。读取时先查 Metadata CF，未找到则回退查 DATA_CF（旧编码）。这种"读时回退 + 写时迁移"的策略让我们不需要一次性迁移所有数据——旧数据随着正常的读写操作逐渐迁移到新编码。

## Metadata CF 编码

Metadata CF 中每个 Redis key 对应一条记录。key 的格式和上一章描述的统一前缀完全一致：`[region_id:8][index_id:8][slot:2][user_key_len:4][user_key]`。注意与 Phase 1 的区别——末尾没有类型标记字节了，类型信息转移到了 value 中。

value 的第一个字节是 flags，它的结构是这样的：最高位（bit 7，`0x80`）始终为 1，标识这是 64 位编码格式；bits 4-6 保留；bits 0-3（`0x0F` 掩码）存储 Redis 类型枚举——`kRedisNone=0`、`kRedisString=1`、`kRedisHash=2`、`kRedisList=3`、`kRedisSet=4`、`kRedisZSet=5`。

flags 之后的内容因类型而异。

**String** 是单 KV 类型——value 直接内联在 Metadata 中，不需要子键。布局为 `[flags:1][expire_ms:8][inline_value:变长]`。GET/SET 只需一次 RocksDB 读写，这是 String 类型性能最优的关键。

**Hash、Set、ZSet** 的 Metadata value 布局为 `[flags:1][expire_ms:8][version:8][size:8]`。`expire_ms` 是绝对过期时间戳（毫秒），0 表示永不过期。`version` 是 64 位版本号，用于 Lazy Deletion（后面详细讨论）。`size` 是元素数量。

**List** 在此基础上增加了 head 和 tail 指针：`[flags:1][expire_ms:8][version:8][size:8][head:8][tail:8]`。head 和 tail 的初始值都是 `UINT64_MAX / 2`——从中间开始，允许向两端增长。LPUSH 递减 head，RPUSH 递增 tail，两端操作都是 O(1)。这个设计很巧妙：如果从 0 开始，LPUSH 就需要一个负数索引，而 RocksDB 的 key 是无符号字节序，负数索引会排在正数后面。从中间开始，两端都有充足的增长空间——即使一个 List 只做 LPUSH，理论上也能存 `UINT64_MAX / 2`（约 9.2 × 10^18）个元素。

所有多字节字段使用大端编码（Big-Endian），与 key 编码保持一致。编解码实现在 `src/redis/redis_metadata.cpp` 中。

## Subkey CF 编码

复合类型的子元素存储在 DATA_CF 中。每个子元素对应一条 KV 记录。

子元素的 key 格式是在 Metadata key 的基础上追加 `[version:8][sub_key:变长]`。前缀与对应的 Metadata key 完全相同（`region_id` + `index_id` + `slot` + `user_key_len` + `user_key`），这样通过前缀查找就能快速定位某个 Redis key 的所有子元素。`version` 字段是 Lazy Deletion 的核心——只有 version 与 Metadata 中记录的 version 匹配的子键才是有效的。

`sub_key` 的含义因类型而异。对于 Hash，sub_key 是 field name（原始字节），value 是 field value。对于 Set，sub_key 是 member，value 为空——成员信息完全编码在 key 中，RocksDB 的 key 存在性检查就等价于成员存在性检查，这是一个节省空间的巧妙设计。对于 List，sub_key 是大端编码的 uint64 索引，value 是 element 内容。对于 ZSet，sub_key 是 member，value 是 8 字节编码后的 score。

遍历某个 key 的所有子元素时，构建 `[metadata_key_prefix][version]` 作为前缀，使用 RocksDB 的 prefix seek 迭代。比如 `HGETALL mykey` 就是对这个前缀做一次范围扫描，遍历所有 field。

## Score CF 编码

ZSet 需要支持两种查询模式：按 member 查询（`ZSCORE key member`）和按 score 范围查询（`ZRANGEBYSCORE key min max`）。一个 CF 的排序只能满足一种模式，所以 ZSet 使用了双 CF 索引——member 索引在 DATA_CF 中（上一节已介绍），score 索引在 REDIS_ZSET_SCORE_CF 中。

Score CF 的 key 格式为 `[metadata_key_prefix][version:8][score:8][member:变长]`，value 为空——所有信息都编码在 key 中。按 score 范围查询时，构建 `[prefix][version][min_score]` 到 `[prefix][version][max_score]` 的范围，直接利用 RocksDB 的有序迭代。

但这里有一个技术挑战：IEEE 754 double 的字节表示不能直接用于字节序比较。正数之间按字节比较是正确的，但负数之间是反的，而且所有负数的字节表示都大于正数。NeoKV 使用了一种编码让 double 的字节序与数值序一致：

```cpp
void encode_score(double score, std::string* dst) {
    uint64_t bits;
    memcpy(&bits, &score, sizeof(bits));
    if (bits & (1ULL << 63)) {
        bits = ~bits;           // 负数：全部取反
    } else {
        bits ^= (1ULL << 63);  // 正数：翻转符号位
    }
    PutFixed64BigEndian(dst, bits);
}
```

为什么负数要全部取反而正数只翻转符号位？这与 IEEE 754 的二进制表示有关。正数之间，指数和尾数的字节比较顺序与数值顺序一致，只需要翻转符号位（从 0 变 1）让正数排在负数后面。但负数在 IEEE 754 中有一个特殊性质：`-1.0` 的位表示 > `-0.5` 的位表示，而数值上 `-1.0 < -0.5`——也就是说负数的位表示与数值是反序的。全部取反正好把这个反序翻转过来。编码后，`-inf < -1.0 < -0.0 < +0.0 < 1.0 < +inf`，字节序与数值序完全一致。

## Version-Based Lazy Deletion

这是从 Kvrocks 移植的核心机制，解决了一个在生产环境中非常实际的问题。

想象一个 Hash key 有 100 万个 field。当执行 `DEL mykey` 时，朴素的做法是遍历并删除 100 万个子键——这会阻塞 Raft apply 线程数秒，在此期间所有写入都被卡住。这在生产环境中是不可接受的。Redis 自身也遇到过同样的问题，所以引入了 `UNLINK` 命令在后台异步删除。NeoKV 用 Lazy Deletion 更优雅地解决了它。

Lazy Deletion 的核心思路：**删除操作只更新 Metadata，不触碰子键**。

```
删除前：
  Metadata CF: mykey → {type=Hash, version=100, size=1000000}
  Data CF:     mykey/v100/field1 → value1
               mykey/v100/field2 → value2
               ... (100 万条)

执行 DEL mykey：
  只删除 Metadata CF 中的 mykey 记录
  Data CF 中的 100 万条子键原封不动

删除后：
  Metadata CF: mykey 不存在
  Data CF:     mykey/v100/field1 → value1  ← "孤儿"子键
               mykey/v100/field2 → value2
               ... (100 万条孤儿，但无人引用)
```

DEL 操作变成了 O(1)——只需删除一条 Metadata 记录。

那些旧 version 的子键变成了"孤儿"。它们虽然仍然存在于 RocksDB 中，但不会被读到——因为读取子键时会比较其 version 与 Metadata 中的 version，不匹配则视为不存在。如果同一个 key 被重新创建（比如再次 `HSET mykey field1 newvalue`），新数据使用新的 version，旧子键的 version 自然不匹配，永远不会被访问到。孤儿子键最终在 compaction 中被清理——虽然目前还需要通过额外机制（如扫描清理），但这些数据不影响正确性，只占用一些磁盘空间。

Version 的生成必须保证全局唯一且单调递增。NeoKV 的实现使用了一个 53-bit 微秒时间戳加 11-bit 原子计数器的组合：

```cpp
static std::atomic<uint64_t> version_counter_;
static const int kVersionCounterBits = 11;

uint64_t generate_version() {
    int64_t timestamp = current_time_us();
    uint64_t counter = version_counter_.fetch_add(1);
    return (static_cast<uint64_t>(timestamp) << kVersionCounterBits)
         + (counter % (1 << kVersionCounterBits));
}
```

53 位时间戳提供微秒级的粗粒度单调性，11 位计数器（0-2047）保证同一微秒内的唯一性。计数器在启动时随机初始化——这是为了避免一个微妙的问题：如果 Follower 被提升为新 Leader，而计数器从 0 开始，有可能在同一微秒内生成与旧 Leader 相同的 version。随机初始化大幅降低了这种冲突的概率。

## 过期机制

NeoKV 的过期机制由两层保证。

**被动过期**是第一层防线。每次读取 key 时，检查 Metadata 中的 `expire_ms`——如果当前时间已经超过过期时间，直接返回 nil，视为 key 不存在。这保证了用户永远不会读到已过期的数据。

**主动清理**是第二层。`RedisTTLCleaner` 后台线程定期扫描 REDIS_METADATA_CF 中的所有 key，发现已过期的就删除其 Metadata 记录（子键通过 Lazy Deletion 机制处理——Metadata 被删除后，子键自然变成孤儿）。清理只在 Leader Region 上执行，避免 Leader/Follower 之间的不一致。每个 Region 每轮最多清理 1000 个过期 key，避免长时间占用资源。扫描间隔可通过 `redis_ttl_cleanup_interval_s` 配置。

你可能会问：为什么不在 REDIS_METADATA_CF 上也注册一个 Compaction Filter 来清理过期 key？这确实是一个可行的优化方向，但有风险——Compaction Filter 绕过了 Raft 共识，直接在本地 RocksDB 上删除数据。如果不同节点的 compaction 时机不同，可能导致副本之间的数据不一致。目前的"被动过期 + 主动清理"方案虽然不够激进，但保证了一致性的安全。

## 编码总览

最后，将整个编码体系汇总，方便查阅：

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

## 检验你的理解

- 为什么 String 类型的值内联在 Metadata 中，而不像其他类型使用子键？如果一个 String 的值非常大（比如 10MB），这种设计还合适吗？
- Version-Based Lazy Deletion 的孤儿子键在 compaction 之前会一直占用磁盘空间。如果系统写入量很大、compaction 跟不上，会有什么影响？
- List 的 head/tail 初始值为 `UINT64_MAX / 2`。如果一个 List 只做 LPUSH（head 不断递减），理论上最多能存多少个元素？
- Score 编码中，负数需要"全部取反"而正数只需"翻转符号位"，为什么？（提示：考虑 IEEE 754 中负数的指数和尾数排列）
- 两层编码系统的迁移策略是"读时回退 + 写时迁移"。如果有一个 key 从来不被写入，只被读取，它会永远留在旧编码中吗？这有什么问题？

---

> Part 2 到此结束。我们已经完整了解了 NeoKV 的存储层：从 RocksDB 的核心特性，到 8 个 CF 的设计，再到 Redis 数据类型的编码方案。
>
> 接下来进入 [Part 3: Redis 数据结构](../3-Redis数据结构/08-String类型.md)，我们将逐一了解五种数据类型的具体实现。
