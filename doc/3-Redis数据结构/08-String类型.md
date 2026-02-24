# String 类型

## 本章概览

在这一章中，我们将了解：

- String 在 NeoKV 中的存储模型：内联编码的优势
- 基础命令的实现：SET/GET/DEL/MGET/MSET
- 数值操作：INCR/DECR/INCRBYFLOAT 的读-改-写模式
- 字符串操作：APPEND/GETRANGE/SETRANGE
- 条件写入：NX/XX/EX/PX 选项的处理
- Phase 1 到 Phase 2 的迁移策略

String 是 Redis 中最基础的数据类型，也是 NeoKV 中性能最优的类型——GET/SET 只需一次 RocksDB 读写。

**关键文件**：
- `src/redis/redis_service.cpp` — 命令 Handler（SetCommandHandler、GetCommandHandler 等）
- `src/redis/region_redis.cpp` — Raft apply 逻辑（REDIS_SET、REDIS_INCR 等）
- `include/redis/redis_metadata.h` — RedisMetadata 编码

## 1. 存储模型

String 是**单 KV 类型**（`IsSingleKVType`），值直接内联在 Metadata CF 的 value 中：

```
REDIS_METADATA_CF:
  Key:   [region_id:8][index_id:8][slot:2][key_len:4]["mykey"]
  Value: [flags:1][expire_ms:8]["hello world"]
                                 └── 内联 value
```

不需要子键，不需要 version。一次 `RocksDB::Get()` 就能拿到完整的值。

对比复合类型（如 Hash）需要先读 Metadata 再读子键，String 的读写路径是最短的。

## 2. 基础命令

### 2.1 SET

SET 是最核心的写命令，支持丰富的选项：

```
SET key value [EX seconds] [PX milliseconds] [NX|XX] [KEEPTTL] [GET]
```

**Handler 层**（`redis_service.cpp`）：

```cpp
// SetCommandHandler::Run() — 简化逻辑
void Run() {
    // 1. 解析参数：key, value, EX/PX, NX/XX, KEEPTTL, GET
    parse_set_options(args, &expire_ms, &condition, &keepttl, &get_old);

    // 2. 路由到 Region
    auto result = check_route(slot);

    // 3. 如果有 GET 选项，先读取旧值
    if (get_old) {
        read_key(key, &old_value);
    }

    // 4. 构建 RedisWriteRequest，提交 Raft
    pb::RedisWriteRequest write_req;
    write_req.set_cmd(pb::REDIS_SET);
    write_req.add_kvs()->set_key(key);
    write_req.mutable_kvs(0)->set_value(value);
    write_req.set_set_condition(condition);  // NONE / NX / XX
    write_through_raft(region, write_req, &response);
}
```

**Apply 层**（`region_redis.cpp`）：

```cpp
// Region::apply_redis_write() — REDIS_SET 分支
case pb::REDIS_SET: {
    for (auto& kv : request.kvs()) {
        // 1. 读取现有 Metadata（检查 NX/XX 条件）
        RedisMetadata metadata;
        bool exists = read_metadata(key, &metadata);

        // 2. 检查条件
        if (condition == NX && exists && !metadata.is_expired()) {
            continue;  // key 已存在，NX 条件不满足
        }
        if (condition == XX && (!exists || metadata.is_expired())) {
            continue;  // key 不存在，XX 条件不满足
        }

        // 3. 构建新的 Metadata
        RedisMetadata new_meta(kRedisString, false);
        new_meta.expire_ms = expire_ms;

        // 4. 编码并写入
        std::string meta_value = new_meta.encode();
        meta_value.append(kv.value());  // 内联 value
        batch.Put(metadata_cf, encoded_key, meta_value);

        // 5. 如果旧数据在 Data CF（Phase 1），删除旧 key
        batch.Delete(data_cf, old_encoded_key);

        affected_rows++;
    }
    db->Write(write_options, &batch);
    break;
}
```

注意 NX/XX 条件检查在 **apply 层**执行，而不是 handler 层。这是因为条件检查必须在 Raft apply 的串行执行中进行，才能保证并发安全。如果在 handler 层检查，两个并发的 `SET key value NX` 可能都认为 key 不存在，都提交 Raft 日志，导致两个都成功。

### 2.2 GET

GET 是纯读操作，不经过 Raft：

```cpp
// GetCommandHandler::Run()
void Run() {
    auto result = check_route(slot);
    check_read_consistency(region);  // Leader 直读 / Follower ReadIndex

    // 读取 Metadata CF
    std::string value;
    auto status = read_key(key, &value);

    if (status.ok()) {
        output->SetBulkString(value);
    } else {
        output->SetNullString();
    }
}
```

`read_key()` 内部实现了两层回退：

```cpp
// RedisDataCommandHandler::read_key() — 简化逻辑
Status read_key(const std::string& user_key, std::string* value) {
    // 1. 先查 Metadata CF
    auto meta_key = RedisMetadataKey(region_id, index_id, slot, user_key).encode();
    auto s = db->Get(read_options, metadata_cf, meta_key, &raw_value);

    if (s.ok()) {
        RedisMetadata metadata;
        metadata.decode(raw_value);
        if (metadata.is_expired()) return Status::NotFound();
        if (metadata.type != kRedisString) return Status::InvalidArgument("WRONGTYPE");
        // 内联 value 在 flags(1) + expire_ms(8) 之后
        *value = raw_value.substr(9);
        return Status::OK();
    }

    // 2. 回退到 Data CF（Phase 1 旧编码）
    auto old_key = RedisCodec::encode_key(region_id, index_id, slot, user_key, REDIS_STRING);
    s = db->Get(read_options, data_cf, old_key, &raw_value);
    if (s.ok()) {
        // 检查过期
        int64_t expire_at = decode_expire(raw_value);
        if (expire_at > 0 && current_time_ms() >= expire_at) return Status::NotFound();
        *value = raw_value.substr(8);  // 跳过 expire_at_ms(8)
        return Status::OK();
    }

    return Status::NotFound();
}
```

### 2.3 MGET / MSET

多 key 操作要求所有 key 在同一个 slot（CROSSSLOT 校验）：

```
redis> MSET {user}:name Alice {user}:age 30    # OK，{user} hash tag 保证同 slot
redis> MSET name Alice age 30                   # 可能报 CROSSSLOT 错误
```

MSET 通过一次 Raft 提交处理所有 KV 对，WriteBatch 保证原子性。

### 2.4 DEL / UNLINK

DEL 和 UNLINK 在 NeoKV 中行为相同（都是同步删除 Metadata）：

```cpp
case pb::REDIS_DEL: {
    for (auto& kv : request.kvs()) {
        // 读取 Metadata，检查是否存在
        if (exists && !metadata.is_expired()) {
            batch.Delete(metadata_cf, encoded_key);
            // 如果是复合类型，子键通过 Lazy Deletion 处理
            // 如果是旧编码 String，也删除 Data CF 的 key
            batch.Delete(data_cf, old_encoded_key);
            affected_rows++;
        }
    }
}
```

## 3. 数值操作

INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT 都遵循**读-改-写**模式。

### 3.1 INCR 的实现

```cpp
case pb::REDIS_INCR: {
    // 1. 读取当前值
    std::string current_value;
    bool exists = read_string_value(key, &current_value);

    // 2. 解析为整数
    int64_t current = 0;
    if (exists) {
        if (!safe_strtoll(current_value, &current)) {
            // 值不是整数，返回错误
            response->set_errcode(PARSE_ERROR);
            response->set_errmsg("value is not an integer");
            return;
        }
    }

    // 3. 计算新值（检查溢出）
    int64_t increment = request.kvs(0).ttl();  // increment 复用 ttl 字段传递
    if (would_overflow(current, increment)) {
        response->set_errmsg("increment would produce overflow");
        return;
    }
    int64_t result = current + increment;

    // 4. 写回
    RedisMetadata new_meta(kRedisString, false);
    new_meta.expire_ms = exists ? old_expire : 0;  // 保留原有 TTL
    std::string meta_value = new_meta.encode();
    meta_value.append(std::to_string(result));
    batch.Put(metadata_cf, encoded_key, meta_value);

    // 5. 返回新值
    response->set_errmsg(std::to_string(result));
}
```

关键点：
- **在 Raft apply 中执行读-改-写**：保证并发安全，不会出现 lost update
- **保留原有 TTL**：INCR 不改变 key 的过期时间
- **溢出检测**：防止 int64 溢出

### 3.2 INCRBYFLOAT

浮点数操作类似，但使用 `long double` 进行计算以保持精度：

```cpp
case pb::REDIS_INCRBYFLOAT_STR: {
    long double current = 0.0;
    if (exists) {
        current = strtold(current_value.c_str(), nullptr);
    }
    long double increment = strtold(request.kvs(0).value().c_str(), nullptr);
    long double result = current + increment;

    // 格式化结果（去除尾部零）
    std::string result_str = format_double(result);
    // ... 写回
}
```

## 4. 字符串操作

### 4.1 APPEND

在现有值末尾追加字符串：

```cpp
case pb::REDIS_APPEND: {
    std::string current_value;
    bool exists = read_string_value(key, &current_value);

    // 追加
    std::string new_value = exists ? current_value + append_value : append_value;

    // 写回（保留原有 TTL）
    RedisMetadata new_meta(kRedisString, false);
    new_meta.expire_ms = exists ? old_expire : 0;
    std::string meta_value = new_meta.encode();
    meta_value.append(new_value);
    batch.Put(metadata_cf, encoded_key, meta_value);

    // 返回新长度
    response->set_affected_rows(new_value.size());
}
```

### 4.2 GETRANGE / SETRANGE

GETRANGE 是纯读操作，在 handler 层直接处理：

```cpp
// GetrangeCommandHandler::Run()
void Run() {
    read_key(key, &value);
    // 处理负数索引
    int64_t start = normalize_index(args[2], value.size());
    int64_t end = normalize_index(args[3], value.size());
    output->SetBulkString(value.substr(start, end - start + 1));
}
```

SETRANGE 需要通过 Raft（修改数据）：

```cpp
case pb::REDIS_SETRANGE: {
    std::string current_value;
    read_string_value(key, &current_value);

    int64_t offset = request.kvs(0).ttl();  // offset 复用 ttl 字段
    std::string patch = request.kvs(0).value();

    // 如果 offset 超出当前长度，用 \0 填充
    if (offset + patch.size() > current_value.size()) {
        current_value.resize(offset + patch.size(), '\0');
    }
    current_value.replace(offset, patch.size(), patch);

    // 写回
    // ...
}
```

### 4.3 GETSET / GETDEL / GETEX

这三个命令都是"读旧值 + 执行操作"的模式：

| 命令 | 行为 |
|------|------|
| GETSET key value | 返回旧值，设置新值 |
| GETDEL key | 返回旧值，删除 key |
| GETEX key [EX/PX/PERSIST] | 返回旧值，修改过期时间 |

它们都是写命令（通过 Raft），旧值通过 `response->set_errmsg()` 返回——这是一个巧妙的 hack，复用了 errmsg 字段来传递 Raft apply 的返回值。

## 5. 过期相关命令

### 5.1 EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT

这些命令修改 key 的过期时间：

```cpp
case pb::REDIS_EXPIRE: {
    // 读取现有 Metadata
    if (!exists || metadata.is_expired()) {
        affected_rows = 0;  // key 不存在
        break;
    }

    // 更新过期时间
    metadata.expire_ms = new_expire_ms;

    // 重新编码并写入
    std::string meta_value = metadata.encode();
    if (metadata.is_single_kv_type()) {
        meta_value.append(inline_value);  // String 需要保留内联值
    }
    batch.Put(metadata_cf, encoded_key, meta_value);
    affected_rows = 1;
}
```

### 5.2 TTL / PTTL

纯读操作，返回剩余过期时间：

```cpp
// TtlCommandHandler::Run()
void Run() {
    auto s = db->Get(read_options, metadata_cf, encoded_key, &raw_value);
    if (!s.ok()) { output->SetInteger(-2); return; }  // key 不存在

    RedisMetadata metadata;
    metadata.decode(raw_value);
    if (metadata.is_expired()) { output->SetInteger(-2); return; }
    if (metadata.expire_ms == 0) { output->SetInteger(-1); return; }  // 无过期

    int64_t ttl = (metadata.expire_ms - current_time_ms()) / 1000;  // TTL 返回秒
    output->SetInteger(std::max(ttl, 0LL));
}
```

### 5.3 PERSIST

移除 key 的过期时间（设为永不过期）：

```cpp
case pb::REDIS_PERSIST: {
    if (metadata.expire_ms == 0) {
        affected_rows = 0;  // 本来就没有过期时间
        break;
    }
    metadata.expire_ms = 0;
    // 重新编码写入...
    affected_rows = 1;
}
```

## 6. 迁移策略

String 类型经历了从 Phase 1（Data CF）到 Phase 2（Metadata CF）的迁移：

```
Phase 1 (旧):
  DATA_CF: [prefix][user_key]['S'] → [expire_ms:8][value]

Phase 2 (新):
  REDIS_METADATA_CF: [prefix][user_key] → [flags:1][expire_ms:8][value]
```

迁移规则：
1. **所有新写入**都写入 Metadata CF
2. **写入时**，如果 Data CF 中存在旧 key，同时删除它
3. **读取时**，先查 Metadata CF；未找到则回退查 Data CF
4. **FlushDB** 同时清理两个 CF

这种策略保证了：
- 不需要停机迁移
- 旧数据随着正常读写逐渐迁移
- 两种编码在过渡期内可以共存

## 7. 已实现命令一览

| 命令 | 类型 | 说明 |
|------|------|------|
| SET | 写 | 支持 EX/PX/NX/XX/KEEPTTL/GET |
| GET | 读 | |
| DEL | 写 | 支持多 key（需同 slot） |
| UNLINK | 写 | 与 DEL 行为相同 |
| MSET | 写 | 多 key 原子写入 |
| MGET | 读 | 多 key 批量读取 |
| SETNX | 写 | 等价于 SET key value NX |
| SETEX | 写 | 等价于 SET key value EX seconds |
| PSETEX | 写 | 等价于 SET key value PX milliseconds |
| INCR / DECR | 写 | 整数 +1 / -1 |
| INCRBY / DECRBY | 写 | 整数 +n / -n |
| INCRBYFLOAT | 写 | 浮点数增量 |
| APPEND | 写 | 追加字符串 |
| STRLEN | 读 | 返回值长度 |
| GETRANGE | 读 | 返回子串 |
| SETRANGE | 写 | 覆盖子串 |
| GETSET | 写 | 返回旧值，设置新值 |
| GETDEL | 写 | 返回旧值，删除 key |
| GETEX | 写 | 返回旧值，修改过期时间 |
| EXISTS | 读 | 检查 key 是否存在 |
| TYPE | 读 | 返回 key 的类型 |
| TTL / PTTL | 读 | 返回剩余过期时间 |
| EXPIRE / PEXPIRE | 写 | 设置过期时间 |
| EXPIREAT / PEXPIREAT | 写 | 设置绝对过期时间 |
| PERSIST | 写 | 移除过期时间 |

## 检验你的理解

- SET 命令的 NX 条件检查为什么必须在 Raft apply 层执行，而不能在 handler 层？如果在 handler 层检查会有什么并发问题？
- INCR 操作需要先读后写。在 Raft apply 中，这个读-改-写是原子的吗？为什么？
- MSET 如何保证原子性？如果 WriteBatch 中途失败会怎样？
- APPEND 保留了原有的 TTL。如果一个 key 即将过期，APPEND 操作会延长它的生命周期吗？
- 迁移策略中，如果一个 key 只被读取从不被写入，它会永远留在旧编码中。这会造成什么问题？有什么解决方案？

---

> 下一章：[09-Hash 类型](./09-Hash类型.md) — 我们将了解第一个复合类型的实现，包括子键编码和 field 操作。
