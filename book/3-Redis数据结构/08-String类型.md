# String 类型

## 概览

- String 在 NeoKV 中的存储模型：内联编码的优势
- SET/GET 的完整路径：从 Handler 到 Raft Apply
- 数值操作的读-改-写模式与并发安全
- NX/XX 条件写入为什么必须在 Apply 层执行
- Phase 1 到 Phase 2 的平滑迁移

String 是 Redis 中最基础的数据类型，也是 NeoKV 中性能最优的类型——GET/SET 只需一次 RocksDB 读写。理解了 String 的实现，你就掌握了 NeoKV 中"命令从网络到磁盘"的完整链路。

## 最简单的类型，最短的路径

上一章我们详细介绍了 Redis 存储编码，提到 String 是**单 KV 类型**——值直接内联在 Metadata CF 的 value 中，不需要子键，不需要 version。一次 `RocksDB::Get()` 就能拿到完整的值。对比复合类型（Hash 需要先读 Metadata 再读子键），String 的读写路径是最短的。

```
REDIS_METADATA_CF:
  Key:   [region_id:8][index_id:8][slot:2][key_len:4]["mykey"]
  Value: [flags:1][expire_ms:8]["hello world"]
                                 └── 内联 value
```

这个设计意味着 SET 只需要一次 `batch.Put()`，GET 只需要一次 `rocks->get()`。没有中间层，没有二次查找。在五种数据类型中，String 是唯一不需要 version 字段的——因为它没有子键，也就不存在 Lazy Deletion 的需求。

## SET：一个写命令的完整旅程

SET 是最核心的写命令，支持丰富的选项：`SET key value [EX seconds] [PX milliseconds] [NX|XX] [KEEPTTL] [GET]`。让我们跟着一条 SET 命令，走完它从 Handler 到 Apply 的全过程。

**Handler 层**（`SetCommandHandler` in `redis_service.cpp`）负责解析参数。它从命令参数中提取 key、value、过期时间、条件标志（NX/XX），然后调用 `write_through_raft()` 将请求序列化为 protobuf，提交给 Raft。Handler 层不做任何数据读取或条件判断——它只是一个"打包工"。

```cpp
write_through_raft(route, pb::REDIS_SET, kvs, expire_at_ms, output, &affected, set_condition);
```

**Apply 层**（`region_redis.cpp`）是真正执行写入的地方。当 Raft 日志被多数节点确认后，`on_apply()` 被调用，最终走到 `REDIS_SET` 分支：

```cpp
case pb::REDIS_SET:
case pb::REDIS_MSET: {
    if (cmd == pb::REDIS_SET && set_condition != pb::REDIS_SET_CONDITION_NONE) {
        int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, nullptr, nullptr);
        bool key_exists = (ret == 0);
        if ((set_condition == pb::REDIS_SET_CONDITION_NX && key_exists) ||
            (set_condition == pb::REDIS_SET_CONDITION_XX && !key_exists)) {
            break;
        }
    }
    std::string meta_value = encode_string_metadata_value(expire_ms, kv.value());
    batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
    batch.Delete(rocks->get_data_handle(), old_key);
    ++num_affected;
    break;
}
```

这里有一个非常重要的设计决策：**NX/XX 条件检查在 Apply 层执行，而不是 Handler 层**。为什么？因为 Apply 是 Raft 保证的串行执行——同一个 Region 的 `on_apply()` 不会并发调用。如果在 Handler 层检查条件，两个并发的 `SET key value NX` 可能都发现 key 不存在，都提交 Raft 日志，最终两个都成功——这就违反了 NX 的语义。把条件检查放在 Apply 层，就利用了 Raft 的串行 apply 保证来实现并发安全。

还有一个细节值得注意：每次写入新编码时，都会同时 `batch.Delete()` 旧编码的 DATA_CF key。这就是 Phase 1 到 Phase 2 迁移的"写时清理"——如果这个 key 之前用旧编码写过，趁这次写入把旧数据清掉。

## GET：纯读操作与两层回退

GET 是纯读操作，不经过 Raft，在 Handler 层直接执行。`GetCommandHandler` 调用 `read_key()` 读取 Metadata CF，返回内联的 value。

但 `read_string_key()` 的实现比你想象的要复杂一点——它实现了 Phase 1 到 Phase 2 的两层回退：

```cpp
static int read_string_key(RocksWrapper* rocks, int64_t region_id, int64_t index_id, 
                           uint16_t slot, const std::string& user_key, 
                           std::string* payload, int64_t* expire_ms) {
    // 先查 Metadata CF（Phase 2 新编码）
    auto status = rocks->get(read_options, rocks->get_redis_metadata_handle(), meta_key, &raw_value);
    if (status.ok()) {
        RedisMetadata meta(kRedisNone, false);
        meta.decode(&ptr, &remaining);
        if (meta.type() != kRedisString) return 1;
        if (meta.is_expired()) return 1;
        if (payload) payload->assign(ptr, remaining);  // 内联 value 在 metadata 之后
        return 0;
    }

    // 回退到 Data CF（Phase 1 旧编码）
    std::string old_key = RedisCodec::encode_key(region_id, index_id, slot, user_key, REDIS_STRING);
    status = rocks->get(read_options, rocks->get_data_handle(), old_key, &raw_value);
    if (status.ok()) {
        RedisCodec::decode_value(raw_value, &old_expire, &old_payload);
        if (RedisCodec::is_expired(old_expire, RedisCodec::current_time_ms())) return 1;
        if (payload) *payload = std::move(old_payload);
        return 0;
    }
    return 1;  // key 不存在
}
```

这个函数是整个 String 类型的基石——不仅 GET 用它，INCR、APPEND、SETRANGE 等所有需要"先读后写"的命令都依赖它来读取当前值。两层回退的策略保证了新旧编码的平滑共存：新数据写在 Metadata CF，旧数据可能还在 Data CF，读取时两个都查，以先找到的为准。

**MGET/MSET** 是批量版本。MSET 通过一次 Raft 提交处理所有 KV 对，WriteBatch 保证原子性。MGET 则在 Handler 层逐个调用 `read_key()`。多 key 操作要求所有 key 在同一个 slot——跨 slot 会报 `CROSSSLOT` 错误。

## 数值操作：在 Raft Apply 中做读-改-写

INCR/DECR/INCRBY/DECRBY 都遵循**读-改-写**模式。这个模式看起来简单，但在分布式系统中要做对并不容易。

```cpp
case pb::REDIS_INCR: {
    std::string payload;
    int64_t old_expire = 0;
    int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);

    int64_t current_val = 0;
    if (ret == 0) {
        current_val = std::stoll(payload, &pos);
    }

    int64_t increment = std::stoll(kv.value());

    // 溢出检测
    if ((increment > 0 && current_val > INT64_MAX - increment) ||
        (increment < 0 && current_val < INT64_MIN - increment)) {
        return;  // 返回错误
    }

    int64_t new_val = current_val + increment;
    std::string meta_value = encode_string_metadata_value(expire_to_use, std::to_string(new_val));
    batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
    batch.Delete(rocks->get_data_handle(), old_key);

    num_affected = new_val;  // 通过 affected_rows 返回新值
    break;
}
```

这段代码的关键在于：**读-改-写三步操作全部在 Raft Apply 中执行**。Apply 是串行的，不会有两个 INCR 同时读到同一个旧值然后各自加一——这是经典的 lost update 问题，Raft 的串行 apply 天然避免了它。

几个值得注意的细节：INCR 保留原有的 TTL（`expire_to_use` 使用旧的过期时间），不会因为自增就改变 key 的生命周期。溢出检测用的是标准的整数溢出判断技巧——不是先加再检查（那样已经溢出了），而是用减法反向判断。返回值通过 `num_affected` 传递回 Handler 层，Handler 从 `response.affected_rows()` 中读取新值返回给客户端。

**INCRBYFLOAT** 的模式类似，但用 `double` 计算，并额外检查 NaN 和 Infinity。返回值不走 `num_affected`（因为那是整数），而是通过 `response.set_errmsg()` 传递——这是一个巧妙的 hack，复用了 errmsg 字段来传递 Raft apply 的返回值。

## 字符串操作：APPEND 和 SETRANGE

**APPEND** 在现有值末尾追加字符串，同样是读-改-写模式：

```cpp
case pb::REDIS_APPEND: {
    std::string payload;
    int64_t old_expire = 0;
    int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);
    if (ret == 1) {
        payload.clear();
        old_expire = REDIS_NO_EXPIRE;
    }
    payload.append(kv.value());
    std::string meta_value = encode_string_metadata_value(old_expire, payload);
    batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
    num_affected = static_cast<int64_t>(payload.size());
    break;
}
```

APPEND 保留了原有的 TTL。如果 key 不存在，等价于 SET——从空字符串开始追加。返回值是新字符串的长度。

**SETRANGE** 覆盖指定偏移量处的子串。offset 复用了 protobuf 的 `expire_ms` 字段传递（这种字段复用在 NeoKV 中很常见，后面其他类型也会看到）。如果 offset 超出当前长度，中间用 `\0` 填充：

```cpp
case pb::REDIS_SETRANGE: {
    int64_t offset = kv.has_expire_ms() ? kv.expire_ms() : 0;
    size_t needed = static_cast<size_t>(offset) + replace_value.size();
    if (needed > payload.size()) {
        payload.resize(needed, '\0');
    }
    memcpy(&payload[offset], replace_value.data(), replace_value.size());
    // 写回...
    break;
}
```

**GETRANGE** 是纯读操作，在 Handler 层直接截取子串返回，不经过 Raft。

## GETSET / GETDEL / GETEX：读旧值 + 执行操作

这三个命令都是"读旧值 + 执行操作"的模式，它们都是写命令（通过 Raft），旧值通过 `response.set_errmsg()` 返回。

**GETSET** 返回旧值，设置新值（同时清除 TTL）。**GETDEL** 返回旧值，删除 key。**GETEX** 返回旧值，修改过期时间（值本身不变）。

Handler 层通过检查 `response.affected_rows()` 来判断 key 是否存在——如果为 0，返回 nil；否则从 `response.errmsg()` 中读取旧值返回给客户端：

```cpp
if (response.affected_rows() == 0) {
    output->SetNullString();
} else {
    output->SetString(response.errmsg());
}
```

用 `errmsg` 来传递正常返回值看起来有点"不讲究"，但在 Raft apply 的架构下，这是一个务实的选择——apply 的返回通道只有 Closure 中的 response 对象，而 response 中能塞自定义字符串的字段就是 errmsg。

## 过期相关命令

**EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT** 修改 key 的过期时间。在 Apply 层，读取现有 Metadata，更新 `expire_ms` 字段，然后重新编码写入。对于 String 类型，重新编码时必须保留内联的 value——读出来的 raw value 包含 flags + expire_ms + value，更新 expire_ms 后需要把 value 部分重新拼上去。

**TTL/PTTL** 是纯读操作。读取 Metadata 中的 `expire_ms`，与当前时间做差，返回剩余秒数（TTL）或毫秒数（PTTL）。key 不存在返回 -2，存在但无过期返回 -1。

**PERSIST** 将过期时间设为 0（永不过期）。如果 key 本来就没有过期时间，返回 0（无操作）。

## DEL：一个看似简单的命令

DEL 在 String 类型上的行为很直接——删除 Metadata CF 中的记录。但实现中需要处理两层编码的兼容：

```cpp
case pb::REDIS_DEL: {
    RedisMetadata del_meta(kRedisNone, false);
    int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &del_meta);
    if (ret == 0) {
        batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
        ++num_affected;
    } else {
        // 回退：检查旧编码 Data CF
        ret = read_string_key(rocks, _region_id, index_id, slot, user_key, nullptr, nullptr);
        if (ret == 0) {
            std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
            batch.Delete(rocks->get_data_handle(), old_key);
            ++num_affected;
        }
    }
    break;
}
```

先查 Metadata CF，找到就删；没找到再查旧编码的 Data CF。注意这里 DEL 是通用命令，不限于 String——如果 key 是 Hash 或 Set 等复合类型，也是删除 Metadata（子键通过 Lazy Deletion 处理，见上一章）。但对 String 来说，删除 Metadata 就是删除全部数据，因为值是内联的。

## 迁移策略

最后总结一下 String 类型从 Phase 1 到 Phase 2 的迁移逻辑。Phase 1 将数据存在 DATA_CF：`[prefix][user_key]['S'] → [expire_ms:8][value]`。Phase 2 存在 REDIS_METADATA_CF：`[prefix][user_key] → [flags:1][expire_ms:8][value]`。

迁移通过四条规则实现：所有新写入都写入 Metadata CF。写入时如果 Data CF 中存在旧 key，同时删除它。读取时先查 Metadata CF，未找到则回退查 Data CF。FlushDB 同时清理两个 CF。

这种"读时回退 + 写时迁移"的策略不需要停机，旧数据随着正常读写逐渐迁移到新编码。但有一个边界情况值得思考：如果一个 key 只被读取从不被写入，它会永远留在旧编码中。在实际生产中这通常不是问题——因为过期机制和 TTL Cleaner 最终会清理它，或者业务迟早会写入新数据。

## 检验你的理解

- SET 命令的 NX 条件检查为什么必须在 Raft apply 层执行？如果在 handler 层检查，两个并发 `SET key value NX` 会出现什么问题？
- INCR 操作的读-改-写是在 Raft apply 中串行执行的。这保证了什么？如果 INCR 不经过 Raft，直接在 handler 层读-改-写会怎样？
- INCRBYFLOAT 通过 `response.errmsg()` 返回新值，而 INCR 通过 `num_affected` 返回。为什么要用两种不同的方式？
- `read_string_key()` 的两层回退意味着每次读取最多可能查两次 RocksDB。这对读性能有什么影响？迁移完成后这个开销还存在吗？
- APPEND 保留了原有的 TTL。如果一个 key 即将过期，APPEND 操作不会延长它的生命周期——这是否符合你的预期？Redis 原生的 APPEND 也是这个行为吗？

---

> 下一章：[09-Hash 类型](./09-Hash类型.md) — 我们将了解第一个复合类型的实现：Metadata + Subkey 的双层结构、size 计数的维护，以及 Lazy Deletion 在 Hash 上的实际运作。
