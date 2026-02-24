# 过期与 TTL

## 本章概览

在这一章中，我们将了解：

- NeoKV 的三层过期机制：被动过期、主动清理、Compaction Filter
- 被动过期：读时检查的实现细节
- RedisTTLCleaner：后台清理线程的扫描策略
- 过期时间的存储与编码
- 过期相关命令的实现

**关键文件**：
- `include/redis/redis_ttl_cleaner.h` — RedisTTLCleaner 单例
- `src/redis/redis_ttl_cleaner.cpp` — 后台清理实现（约 200 行）
- `include/redis/redis_metadata.h` — `expire_ms` 字段、`is_expired()`

## 1. 三层过期机制

Redis 的过期机制是"被动 + 主动"的组合。NeoKV 在此基础上增加了 Compaction Filter 层：

```
┌─────────────────────────────────────────────────────┐
│  第一层：被动过期（读时检查）                          │
│  每次读取 key 时检查 expire_ms                       │
│  过期则返回 nil，但不立即删除                         │
│  延迟：0（实时）  覆盖：100%                         │
├─────────────────────────────────────────────────────┤
│  第二层：主动清理（TTL Cleaner）                      │
│  后台线程定期扫描 Leader Region                      │
│  批量删除过期的 Metadata                             │
│  延迟：60 秒（可配置）  覆盖：Leader Region          │
├─────────────────────────────────────────────────────┤
│  第三层：Compaction Filter（规划中）                  │
│  RocksDB compaction 时清理过期数据                   │
│  延迟：不确定（取决于 compaction 触发时机）           │
│  覆盖：所有数据                                      │
└─────────────────────────────────────────────────────┘
```

三层机制各有侧重：
- **被动过期**保证读取的正确性（永远不会读到过期数据）
- **主动清理**回收过期 key 占用的空间
- **Compaction Filter**清理孤儿子键和深层过期数据

## 2. 过期时间的存储

过期时间以**绝对毫秒时间戳**存储在 Metadata value 中：

```
Metadata Value:
[flags:1][expire_ms:8][...]
                ↑
        绝对时间戳（毫秒）
        0 = 永不过期
```

所有过期相关命令最终都转换为绝对时间戳：

| 命令 | 参数 | 转换 |
|------|------|------|
| EXPIRE key 60 | 相对秒数 | `current_time_ms() + 60 * 1000` |
| PEXPIRE key 5000 | 相对毫秒 | `current_time_ms() + 5000` |
| EXPIREAT key 1700000000 | 绝对秒数 | `1700000000 * 1000` |
| PEXPIREAT key 1700000000000 | 绝对毫秒 | 直接使用 |
| SET key val EX 60 | 相对秒数 | `current_time_ms() + 60 * 1000` |
| SET key val PX 5000 | 相对毫秒 | `current_time_ms() + 5000` |

### 2.1 过期检查

```cpp
// include/redis/redis_metadata.h
bool RedisMetadata::is_expired() const {
    if (expire_ms == 0) return false;  // 永不过期
    return butil::gettimeofday_ms() >= expire_ms;
}

bool RedisMetadata::is_dead() const {
    if (is_expired()) return true;
    // 复合类型 size=0 也视为死亡
    if (!is_single_kv_type() && size == 0) return true;
    return false;
}
```

`is_dead()` 比 `is_expired()` 多了一个条件：空的复合类型（size=0）也被视为不存在。这处理了 HDEL 删除所有 field 后 Metadata 残留的情况。

## 3. 被动过期

每次读取 key 时，都会检查过期状态。这是分散在各个 Handler 和 apply 逻辑中的：

### 3.1 读命令中的过期检查

```cpp
// 通用的 Metadata 读取（所有读命令都会调用）
bool RedisDataCommandHandler::read_metadata(const std::string& key,
                                              RedisMetadata* metadata) {
    auto meta_key = RedisMetadataKey(...).encode();
    std::string raw_value;
    auto s = db->Get(read_options, metadata_cf, meta_key, &raw_value);

    if (!s.ok()) return false;

    metadata->decode(raw_value);

    // 过期检查
    if (metadata->is_expired()) {
        return false;  // 视为不存在
    }

    return true;
}
```

被动过期**不删除数据**——只是在读取时返回"不存在"。实际的数据删除由主动清理完成。

这个设计的原因：读操作不经过 Raft，如果在读时删除数据，会导致 Leader 和 Follower 的数据不一致。

### 3.2 写命令中的过期检查

写命令在 Raft apply 层也会检查过期：

```cpp
// region_redis.cpp — HSET 的 apply 逻辑
case pb::REDIS_HSET: {
    RedisMetadata metadata;
    bool exists = read_metadata(key, &metadata);

    // 过期的 key 视为不存在，创建新的
    if (!exists || metadata.is_expired() || metadata.type != kRedisHash) {
        metadata = RedisMetadata(kRedisHash, true);
        metadata.size = 0;
    }
    // ...
}
```

过期的 key 被当作不存在处理——创建新的 Metadata（新 version），旧数据通过 Lazy Deletion 处理。

## 4. RedisTTLCleaner

`RedisTTLCleaner` 是一个后台单例线程，定期扫描并删除过期的 key。

### 4.1 架构

```cpp
// include/redis/redis_ttl_cleaner.h
class RedisTTLCleaner {
    static RedisTTLCleaner* get_instance();

    void start();   // 启动后台线程
    void stop();    // 停止

private:
    std::thread _thread;
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _stopped = false;

    void run();                    // 主循环
    void clean_expired_keys();     // 执行一轮清理
};
```

### 4.2 主循环

```cpp
// src/redis/redis_ttl_cleaner.cpp
void RedisTTLCleaner::run() {
    while (!_stopped) {
        // 等待指定间隔（默认 60 秒）
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait_for(lock,
            std::chrono::seconds(FLAGS_redis_ttl_cleanup_interval_s),
            [this] { return _stopped; });

        if (_stopped) break;

        clean_expired_keys();
    }
}
```

使用 `condition_variable::wait_for` 而非 `sleep`，这样 `stop()` 可以立即唤醒线程退出。

### 4.3 清理逻辑

```cpp
void RedisTTLCleaner::clean_expired_keys() {
    auto* store = Store::get_instance();

    store->traverse_regions([](SmartRegion region) {
        // 只在 Leader Region 上执行
        if (!region->is_leader()) return;

        auto* db = RocksWrapper::get_instance();
        rocksdb::ReadOptions read_options;
        read_options.fill_cache = false;  // 不污染 Block Cache

        // 扫描该 Region 在 Metadata CF 中的所有 key
        auto prefix = build_region_prefix(region);
        auto* iter = db->NewIterator(read_options, metadata_cf);

        rocksdb::WriteBatch batch;
        int count = 0;

        for (iter->Seek(prefix);
             iter->Valid() && iter->key().starts_with(prefix) && count < 1000;
             iter->Next()) {

            RedisMetadata metadata;
            metadata.decode(iter->value());

            if (metadata.is_expired()) {
                batch.Delete(metadata_cf, iter->key());

                // 如果是旧编码的 String，也删除 Data CF 中的 key
                if (is_old_encoding(iter->key())) {
                    batch.Delete(data_cf, build_old_key(iter->key()));
                }

                count++;
            }
        }

        if (count > 0) {
            db->Write(rocksdb::WriteOptions(), &batch);
        }
    });
}
```

关键设计决策：

- **只在 Leader 上执行**：如果在 Follower 上也删除，会导致 Leader/Follower 数据不一致（删除操作没有经过 Raft）
- **`fill_cache = false`**：扫描过期 key 不应该污染 Block Cache，避免影响正常读写性能
- **每 Region 每轮最多 1000 个**：避免单次清理耗时过长，影响其他操作
- **直接写 RocksDB**：TTL Cleaner 的删除不经过 Raft。这是安全的，因为：
  - 只删除已过期的 key（被动过期已经保证读不到）
  - 只在 Leader 上执行（Follower 的过期数据不影响正确性）
  - 最坏情况下，Follower 上多保留一些过期数据，不影响一致性

### 4.4 配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `redis_ttl_cleanup_interval_s` | 60 | 清理间隔（秒），0 = 禁用 |

## 5. Compaction Filter（规划中）

第三层过期机制通过 RocksDB Compaction Filter 实现，在 compaction 时清理：

- **REDIS_METADATA_CF 中过期的 Metadata**
- **DATA_CF 中孤儿子键**（version 不匹配或 Metadata 已过期/不存在）
- **REDIS_ZSET_SCORE_CF 中孤儿 Score 条目**

这一层目前在规划中，尚未完全实现。当前的过期数据清理主要依赖前两层。

## 6. 过期相关命令

### 6.1 设置过期时间

| 命令 | 参数 | 精度 |
|------|------|------|
| EXPIRE key seconds | 相对秒数 | 秒 |
| PEXPIRE key milliseconds | 相对毫秒 | 毫秒 |
| EXPIREAT key timestamp | 绝对秒数 | 秒 |
| PEXPIREAT key ms-timestamp | 绝对毫秒 | 毫秒 |

这些命令都是写操作（通过 Raft），在 apply 层更新 Metadata 的 `expire_ms` 字段：

```cpp
case pb::REDIS_EXPIRE: {
    RedisMetadata metadata;
    if (!read_metadata(key, &metadata) || metadata.is_expired()) {
        response->set_affected_rows(0);  // key 不存在
        break;
    }

    // 更新过期时间
    metadata.expire_ms = new_expire_ms;

    // 重新编码 Metadata（保留 inline value / version / size 等）
    std::string meta_value = metadata.encode();
    if (metadata.is_single_kv_type()) {
        // String 类型需要追加内联 value
        meta_value.append(inline_value);
    }
    batch.Put(metadata_cf, meta_key, meta_value);
    response->set_affected_rows(1);
    break;
}
```

### 6.2 查询过期时间

| 命令 | 返回值 | 精度 |
|------|--------|------|
| TTL key | 剩余秒数 | 秒 |
| PTTL key | 剩余毫秒 | 毫秒 |

特殊返回值：
- `-1`：key 存在但没有过期时间
- `-2`：key 不存在

### 6.3 移除过期时间

`PERSIST key`：将 `expire_ms` 设为 0（永不过期）。

## 7. 代码导读

| 文件 | 内容 |
|------|------|
| `include/redis/redis_ttl_cleaner.h` | RedisTTLCleaner 单例定义 |
| `src/redis/redis_ttl_cleaner.cpp` | 后台清理线程实现 |
| `include/redis/redis_metadata.h` | `is_expired()`、`is_dead()`、`expire_ms` 字段 |
| `src/redis/region_redis.cpp` | REDIS_EXPIRE、REDIS_PERSIST 的 apply 逻辑 |
| `src/redis/redis_service.cpp` | TtlCommandHandler、ExpireCommandHandler 等 |

## 检验你的理解

- 被动过期不删除数据，只在读时返回"不存在"。这意味着过期的 key 仍然占用磁盘空间。在什么场景下这会成为问题？
- TTL Cleaner 的删除不经过 Raft。如果 Leader 切换发生在清理过程中，会有什么影响？
- TTL Cleaner 每 Region 每轮最多清理 1000 个 key。如果有 100 万个 key 同时过期，需要多久才能全部清理？
- 为什么 TTL Cleaner 使用 `fill_cache = false`？如果不设置这个选项，会有什么影响？
- 如果给 REDIS_METADATA_CF 添加 Compaction Filter 来清理过期 key，需要注意什么？（提示：考虑 String 类型的内联 value 和复合类型的子键）

---

> Part 4 到此结束。我们已经了解了 Redis 协议层的完整实现：命令框架、路由机制和过期管理。
>
> 接下来进入 [Part 5: 测试体系](../part5-测试与运维/16-测试体系.md)，了解如何验证和扩展 NeoKV 的功能。
