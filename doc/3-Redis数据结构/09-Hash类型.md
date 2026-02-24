# Hash 类型

## 本章概览

在这一章中，我们将了解：

- Hash 的存储模型：Metadata + Subkey 的双层结构
- 写命令的实现：HSET/HDEL/HSETNX/HINCRBY 如何维护 size 计数
- 读命令的实现：HGET/HGETALL/HSCAN 的前缀迭代
- Version-Based Lazy Deletion 在 Hash 上的实际应用

Hash 是 NeoKV 中第一个复合类型，理解了它的实现模式，Set/List/ZSet 就很容易举一反三。

**关键文件**：
- `src/redis/redis_service.cpp` — HsetCommandHandler、HgetCommandHandler 等
- `src/redis/region_redis.cpp` — REDIS_HSET、REDIS_HDEL 等 apply 逻辑
- `include/redis/redis_metadata.h` — RedisMetadata、RedisSubkeyKey

## 1. 存储模型

Hash 使用两个 CF 协作存储：

```
REDIS_METADATA_CF (元信息):
  Key:   [prefix]["mykey"]
  Value: [flags=Hash][expire_ms][version=100][size=3]

DATA_CF (子键):
  Key:   [prefix]["mykey"][version=100]["name"]   → "Alice"
  Key:   [prefix]["mykey"][version=100]["age"]    → "30"
  Key:   [prefix]["mykey"][version=100]["city"]   → "Beijing"
```

- **Metadata** 记录类型、过期时间、版本号和 field 数量
- **Subkey** 每个 field 一条记录，key 中包含 version，value 是 field 的值
- **size** 字段在每次 HSET/HDEL 时维护，HLEN 直接返回 size，O(1)

### 1.1 为什么需要 size 字段

如果没有 size，HLEN 需要遍历所有子键来计数——对于百万 field 的 Hash 来说是灾难性的。通过在 Metadata 中维护 size，HLEN 变成了一次 RocksDB Get。

代价是每次 HSET/HDEL 都需要更新 Metadata（额外一次 Put）。但这个代价是值得的——HLEN 的调用频率远高于 HSET。

## 2. 写命令

### 2.1 HSET / HMSET

HSET 可以一次设置多个 field：

```
HSET mykey name Alice age 30 city Beijing
```

Apply 层实现：

```cpp
case pb::REDIS_HSET: {
    // 1. 读取现有 Metadata
    RedisMetadata metadata;
    bool key_exists = read_metadata(key, &metadata);

    if (!key_exists || metadata.is_expired() || metadata.type != kRedisHash) {
        // key 不存在或类型不匹配，创建新的 Hash
        metadata = RedisMetadata(kRedisHash, true);  // generate_version=true
        metadata.size = 0;
    }

    int new_fields = 0;

    // 2. 逐个处理 field
    for (auto& field : request.fields()) {
        // 构建子键 key
        auto subkey = RedisSubkeyKey(region_id, index_id, slot,
                                      user_key, metadata.version,
                                      field.field()).encode();

        // 检查 field 是否已存在
        std::string old_value;
        auto s = db->Get(read_options, data_cf, subkey, &old_value);

        if (!s.ok()) {
            // 新 field，size +1
            metadata.size++;
            new_fields++;
        }

        // 写入子键
        batch.Put(data_cf, subkey, field.value());
    }

    // 3. 更新 Metadata（新的 size）
    batch.Put(metadata_cf, meta_key, metadata.encode());

    // 4. 原子提交
    db->Write(write_options, &batch);

    // 5. 返回新增的 field 数量（不是总修改数）
    response->set_affected_rows(new_fields);
    break;
}
```

注意 HSET 返回的是**新增** field 的数量，不是修改的数量。如果 `HSET mykey name Bob`（name 已存在），返回 0（更新了但没有新增）。

### 2.2 HDEL

```cpp
case pb::REDIS_HDEL: {
    RedisMetadata metadata;
    if (!read_metadata(key, &metadata) || metadata.is_expired()) {
        response->set_affected_rows(0);
        break;
    }

    int deleted = 0;
    for (auto& member : request.members()) {
        auto subkey = RedisSubkeyKey(..., metadata.version, member).encode();

        // 检查 field 是否存在
        std::string old_value;
        if (db->Get(read_options, data_cf, subkey, &old_value).ok()) {
            batch.Delete(data_cf, subkey);
            metadata.size--;
            deleted++;
        }
    }

    if (metadata.size <= 0) {
        // 所有 field 都删除了，删除 Metadata
        batch.Delete(metadata_cf, meta_key);
    } else {
        // 更新 size
        batch.Put(metadata_cf, meta_key, metadata.encode());
    }

    db->Write(write_options, &batch);
    response->set_affected_rows(deleted);
    break;
}
```

当 `size` 降为 0 时，Metadata 也被删除——空 Hash 不应该存在。

### 2.3 HSETNX

只在 field 不存在时设置：

```cpp
case pb::REDIS_HSETNX: {
    // ... 读取 Metadata

    auto subkey = RedisSubkeyKey(..., metadata.version, field).encode();
    std::string old_value;
    if (db->Get(read_options, data_cf, subkey, &old_value).ok()) {
        // field 已存在，不做任何操作
        response->set_affected_rows(0);
        break;
    }

    // field 不存在，写入
    batch.Put(data_cf, subkey, value);
    metadata.size++;
    batch.Put(metadata_cf, meta_key, metadata.encode());
    db->Write(write_options, &batch);
    response->set_affected_rows(1);
    break;
}
```

### 2.4 HINCRBY / HINCRBYFLOAT

与 String 的 INCR 类似，读-改-写模式：

```cpp
case pb::REDIS_HINCRBY: {
    // 1. 读取现有 field 值
    auto subkey = RedisSubkeyKey(..., metadata.version, field).encode();
    std::string old_value;
    bool field_exists = db->Get(read_options, data_cf, subkey, &old_value).ok();

    // 2. 解析为整数
    int64_t current = 0;
    if (field_exists) {
        if (!safe_strtoll(old_value, &current)) {
            response->set_errmsg("hash value is not an integer");
            return;
        }
    }

    // 3. 计算新值
    int64_t result = current + increment;

    // 4. 写回
    batch.Put(data_cf, subkey, std::to_string(result));
    if (!field_exists) {
        metadata.size++;
        batch.Put(metadata_cf, meta_key, metadata.encode());
    }

    response->set_errmsg(std::to_string(result));  // 返回新值
    break;
}
```

## 3. 读命令

### 3.1 HGET / HMGET

单个 field 查询，直接构建子键 key 读取：

```cpp
// HgetCommandHandler::Run()
void Run() {
    // 1. 读取 Metadata
    RedisMetadata metadata;
    if (!read_metadata(key, &metadata) || metadata.is_expired()) {
        output->SetNullString();
        return;
    }

    // 2. 构建子键 key，读取 Data CF
    auto subkey = RedisSubkeyKey(region_id, index_id, slot,
                                  user_key, metadata.version,
                                  field).encode();
    std::string value;
    auto s = db->Get(read_options, data_cf, subkey, &value);

    if (s.ok()) {
        output->SetBulkString(value);
    } else {
        output->SetNullString();
    }
}
```

HMGET 类似，批量查询多个 field，返回数组。

### 3.2 HGETALL / HKEYS / HVALS

这三个命令都需要遍历所有 field，通过前缀迭代实现：

```cpp
// HgetallCommandHandler::Run()
void Run() {
    RedisMetadata metadata;
    if (!read_metadata(key, &metadata) || metadata.is_expired()) {
        output->SetArray(0);
        return;
    }

    // 构建前缀：[metadata_prefix][version]
    auto prefix = RedisSubkeyKey(region_id, index_id, slot,
                                  user_key, metadata.version, "")
                  .encode_prefix();

    auto* iter = db->NewIterator(read_options, data_cf);
    std::vector<std::string> results;

    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        if (!iter->key().starts_with(prefix)) break;

        // 从子键 key 中提取 field name
        std::string field = extract_subkey(iter->key(), prefix.size());

        results.push_back(field);          // HKEYS 只需要这个
        results.push_back(iter->value());  // HVALS 只需要这个
        // HGETALL 两个都要
    }

    output->SetArray(results);
}
```

HKEYS 只返回 field name，HVALS 只返回 value，HGETALL 返回交替的 field-value 对。

### 3.3 HLEN / HEXISTS

HLEN 直接返回 Metadata 中的 size，O(1)：

```cpp
// HlenCommandHandler::Run()
void Run() {
    RedisMetadata metadata;
    if (!read_metadata(key, &metadata) || metadata.is_expired()) {
        output->SetInteger(0);
        return;
    }
    output->SetInteger(metadata.size);
}
```

HEXISTS 构建子键 key，检查是否存在：

```cpp
// HexistsCommandHandler::Run()
void Run() {
    // ... 读取 Metadata
    auto subkey = RedisSubkeyKey(..., metadata.version, field).encode();
    std::string value;
    auto s = db->Get(read_options, data_cf, subkey, &value);
    output->SetInteger(s.ok() ? 1 : 0);
}
```

### 3.4 HRANDFIELD

随机返回一个或多个 field。实现使用 reservoir sampling（蓄水池采样）：

```cpp
// 简化逻辑
void Run() {
    // 前缀迭代所有 field
    // 使用 reservoir sampling 随机选择 count 个
    // 如果 count 为负数，允许重复
}
```

### 3.5 HSCAN

游标迭代，兼容 Redis 的 SCAN 语义：

```
HSCAN mykey 0 MATCH pattern COUNT 10
```

游标本质上是上次迭代停止的位置。NeoKV 将游标编码为子键的偏移量，下次从该位置继续迭代。

## 4. Lazy Deletion 实战

当执行 `DEL mykey`（mykey 是一个有 100 万 field 的 Hash）时：

```
执行前：
  Metadata: mykey → {version=100, size=1000000}
  Data CF:  mykey/v100/field1 → value1
            mykey/v100/field2 → value2
            ... (100 万条)

执行 DEL mykey：
  Metadata: 删除 mykey 的 Metadata 记录
  Data CF:  不做任何操作（100 万条子键仍然存在）

执行后读取 HGET mykey field1：
  1. 查 Metadata → 不存在 → 返回 nil
  （不会去查 Data CF 中的子键）

执行 HSET mykey field1 newvalue：
  1. 创建新 Metadata: {version=200, size=1}
  2. 写入子键: mykey/v200/field1 → newvalue
  3. 旧子键 mykey/v100/* 仍然存在，但 version=100 ≠ 200，永远不会被读到

最终清理：
  RocksDB compaction 时，Compaction Filter 检查每个子键的 version
  发现 version=100 的子键对应的 Metadata 已不存在 → 删除
```

整个 DEL 操作只需要一次 Metadata 删除，耗时微秒级，不会阻塞 Raft apply。

## 5. 已实现命令一览

| 命令 | 类型 | 说明 |
|------|------|------|
| HSET | 写 | 设置一个或多个 field |
| HMSET | 写 | 与 HSET 相同（Redis 兼容） |
| HGET | 读 | 获取单个 field |
| HMGET | 读 | 批量获取多个 field |
| HDEL | 写 | 删除一个或多个 field |
| HGETALL | 读 | 获取所有 field-value 对 |
| HKEYS | 读 | 获取所有 field name |
| HVALS | 读 | 获取所有 value |
| HLEN | 读 | 返回 field 数量（O(1)） |
| HEXISTS | 读 | 检查 field 是否存在 |
| HSETNX | 写 | 仅在 field 不存在时设置 |
| HINCRBY | 写 | field 值整数增量 |
| HINCRBYFLOAT | 写 | field 值浮点增量 |
| HRANDFIELD | 读 | 随机返回 field |
| HSCAN | 读 | 游标迭代 |

## 6. 代码导读

| 文件 | 关注点 |
|------|--------|
| `src/redis/redis_service.cpp` | 搜索 `HsetCommandHandler`、`HgetCommandHandler` 等 |
| `src/redis/region_redis.cpp` | 搜索 `REDIS_HSET`、`REDIS_HDEL`、`REDIS_HSETNX` 等 |
| `include/redis/redis_metadata.h` | `RedisSubkeyKey` 类：子键编码 |
| `src/redis/redis_metadata.cpp` | `RedisSubkeyKey::encode()` / `encode_prefix()` |

## 检验你的理解

- HSET 返回的是新增 field 数还是总修改数？这在 apply 层如何计算？为什么 Redis 选择返回新增数？
- 删除一个有百万 field 的 Hash 时，Lazy Deletion 让 DEL 变成 O(1)。但孤儿子键占用的磁盘空间什么时候才能释放？
- HGETALL 需要遍历所有子键。如果一个 Hash 有 100 万个 field，这个操作的性能如何？有什么优化建议？
- HSCAN 的游标在 NeoKV 中如何实现？与 Redis 原生的 HSCAN 游标有什么区别？
- 如果两个客户端同时对同一个 Hash 执行 HSET（不同的 field），会有并发问题吗？为什么？

---

> 下一章：[10-Set 类型](./10-Set类型.md) — 我们将了解 Set 的存储模型和集合运算的实现。
