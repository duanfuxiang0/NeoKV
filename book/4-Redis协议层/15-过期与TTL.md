# 过期与 TTL

## 概览

- NeoKV 的两层过期机制：被动过期与主动清理
- 被动过期：为什么读时不删除、只过滤
- RedisTTLCleaner：后台清理的扫描策略与安全性
- 过期时间的存储编码与命令实现
- 为什么 TTL Cleaner 可以绕过 Raft

Redis 最经典的特性之一就是给 key 设置过期时间。`SET session abc EX 3600` 表示这个 session 一小时后自动消失。听起来简单，但在分布式存储引擎上实现"自动消失"并不容易——过期检查发生在什么时机？过期的数据何时真正被删除？删除操作需要经过 Raft 吗？

NeoKV 使用两层过期机制来回答这些问题。

## 过期时间的存储

所有过期相关的信息都编码在 Metadata value 中。无论是 String 还是复合类型，Metadata 的前 9 个字节是固定的：`[flags:1][expire_ms:8]`。`expire_ms` 是一个 `uint64_t`，存储的是**绝对毫秒时间戳**——从 Unix epoch（1970-01-01T00:00:00Z）算起的毫秒数。值为 0 表示永不过期（常量 `REDIS_NO_EXPIRE`）。

所有过期相关命令最终都被转换为绝对时间戳。`EXPIRE key 60` 转换为 `current_time_ms() + 60 * 1000`；`PEXPIRE key 5000` 转换为 `current_time_ms() + 5000`；`EXPIREAT key 1700000000` 转换为 `1700000000 * 1000`（秒转毫秒）；`PEXPIREAT key 1700000000000` 直接使用。`SET key val EX 60` 和 `SET key val PX 5000` 也做同样的转换，在 Handler 层计算好绝对时间戳后通过 `write_through_raft()` 传入。

使用绝对时间戳而非相对时间的原因很直观——如果存储相对时间，每次读取都需要知道"这个相对时间是相对于哪个时刻的"，而绝对时间戳让过期检查变成一个简单的比较：`now >= expire_ms`。

## 第一层：被动过期

被动过期的规则简洁到只有一行代码：

```cpp
bool RedisMetadata::is_expired(int64_t now_ms) const {
    if (expire == 0) return false;
    return now_ms >= static_cast<int64_t>(expire);
}
```

每次读取 key 时——无论是显式的 GET、HGET、ZSCORE，还是写命令 apply 层中隐式的"先读后写"（HSET 需要先读 Metadata 确认类型和 size）——都会调用这个方法。如果返回 true，key 被当作不存在处理。GET 返回 nil，HSET 创建新的 Metadata（新 version，旧子键通过 Lazy Deletion 处理），TTL 返回 -2。

被动过期**不删除数据**。这个设计看起来浪费——过期的 key 仍然占用磁盘空间——但在分布式系统中是必要的。原因很简单：读操作不经过 Raft。如果在读时删除数据，Leader 上的删除不会同步到 Follower，导致 Leader 和 Follower 的数据不一致。考虑这个场景：Leader 上读取 key A，发现过期，删除。然后 Leader 切换到另一个节点。新 Leader 读取 key A——它还在那里，因为旧 Leader 的删除没有通过 Raft 同步。

所以被动过期只负责**读取正确性**——保证客户端永远不会读到过期数据。实际的空间回收由第二层处理。

`RedisMetadata` 还有一个 `is_dead()` 方法，比 `is_expired()` 多一个条件：

```cpp
bool RedisMetadata::is_dead() const {
    if (is_expired()) return true;
    if (!is_single_kv_type() && size == 0) return true;
    return false;
}
```

空的复合类型（Hash/Set/List/ZSet 的 size 为 0）也被视为"死亡"。这处理了一个边界情况：`HDEL key field1 field2 ... fieldN` 逐个删除 Hash 的所有 field 后，Metadata 还在，但 size 已经变成 0。`is_dead()` 确保这种空壳 key 也被当作不存在。

## 第二层：RedisTTLCleaner

`RedisTTLCleaner`（`include/redis/redis_ttl_cleaner.h`）是一个后台单例线程，定期扫描并删除过期的 key，回收第一层被动过期"放过"的磁盘空间。

它的生命周期由 `init()` 和 `shutdown()` 管理。`init()` 接受一个清理间隔（默认 60 秒，由 `FLAGS_redis_ttl_cleanup_interval_s` 控制），启动后台线程。线程主循环使用 `condition_variable::wait_for()` 等待指定间隔，然后执行一轮清理。使用条件变量而非 `sleep()` 让 `shutdown()` 可以立即唤醒线程退出，而不用等到 sleep 结束。将间隔设为 0 可以禁用 TTL Cleaner。

清理逻辑在 `do_cleanup()` 中，核心流程如下：

遍历 Store 中所有 Region，**只在 Leader Region 上执行清理**。对每个 Leader Region，构建该 Region 在数据 CF 中的前缀，创建一个 `fill_cache = false` 的 RocksDB 迭代器，逐条扫描。对每条记录，解码出 `expire_at_ms`，与当前时间比较。如果已过期，将删除操作加入 `WriteBatch`。每批最多 1000 条——避免单次清理耗时过长。批次满或遍历结束时，将 WriteBatch 写入 RocksDB。

几个设计决策值得深入讨论。

**只在 Leader 上执行**。这是最重要的约束。TTL Cleaner 的删除操作**不经过 Raft**——它直接写 RocksDB。如果在 Follower 上也执行，Leader 和 Follower 就会出现数据不一致（某些 key 在 Follower 上被删了但 Leader 上还在）。只在 Leader 上执行绕过了这个问题——Follower 上的过期数据不会被主动清理，但被动过期保证它们在读取时不可见。最坏情况下，Follower 上多保留一些过期数据，浪费一些磁盘空间，不影响正确性。Leader 转移后，新 Leader 会在下一个清理周期接管清理工作。

**为什么可以绕过 Raft？** 这是一个关键的设计判断。通过 Raft 删除过期 key 更"正确"——所有副本保持一致。但代价是每次清理都产生 Raft 日志，增加 Raft 的负担。NeoKV 选择绕过 Raft 的理由是：被动过期已经保证了**语义正确性**（客户端永远不会读到过期数据），TTL Cleaner 只是做**空间回收**。空间回收不一致是可以容忍的——Follower 上多占一些磁盘空间，远比 Raft 日志膨胀的代价小。

**`fill_cache = false`**。TTL Cleaner 的扫描是全量遍历——它需要检查每个 key 的 expire_ms。如果这些数据被加载到 Block Cache 中，会把热点数据（正常读写路径使用的数据）挤出去。`fill_cache = false` 告诉 RocksDB：这次读取的数据不要放进 cache。这是后台任务的标准实践。

**每批 1000 条**。限制单批删除数量是为了控制清理的延迟。如果一次删除 100 万个过期 key，WriteBatch 本身的内存占用、RocksDB 的写入延迟都会受到影响。分批处理让清理均匀分布在多个间隔周期中。如果有 100 万个 key 同时过期，按每 60 秒清理 1000 个计算，需要约 1000 分钟才能全部清理——这似乎很慢，但别忘了被动过期已经保证了这些 key 在读取时不可见。空间回收不那么紧急。

TTL Cleaner 还维护了一些统计信息——总清理数、上一次清理的数量和时间——通过 `get_total_cleaned()` 等方法暴露。这些统计对监控和调试很有用。

## 过期相关命令

**设置过期时间**的命令（EXPIRE、PEXPIRE、EXPIREAT、PEXPIREAT）都是写操作，通过 Raft apply。Handler 层将参数转换为绝对毫秒时间戳，通过 `write_through_raft()` 提交。apply 层的逻辑是：读取现有的 Metadata 和 value，更新 `expire_ms`，重新编码 Metadata value，写回 Metadata CF。如果 key 不存在或已过期，返回 0（表示"未设置"）；成功设置返回 1。

对于 String 类型，apply 层在更新 expire_ms 时还需要保留内联 value——因为 String 的值存储在 Metadata value 的 expire_ms 之后。重新编码时调用 `encode_string_metadata_value(new_expire_ms, payload)` 构建完整的 value。同时，如果存在旧编码（Phase 1）的 Data CF key，也要清理掉。

**查询过期时间**的命令（TTL、PTTL）是读操作，不经过 Raft。Handler 通过 `read_key()` 获取 `expire_at_ms`，然后计算剩余时间。TTL 返回秒（`(expire_at_ms - now) / 1000`），PTTL 返回毫秒（`expire_at_ms - now`）。特殊返回值：-1 表示 key 存在但没有过期时间，-2 表示 key 不存在。如果 key 已过期但尚未被清理，TTL/PTTL 返回 -2——因为 `read_key()` 中的被动过期检查已经将它视为不存在了。

`RedisCodec::compute_ttl_seconds()` 是 TTL 的计算辅助函数。它处理了三种情况：`expire_at_ms == REDIS_NO_EXPIRE` 返回 -1；`now >= expire_at_ms` 返回 0（已过期但 key 还存在的极端情况，理论上不应该发生因为被动过期会先拦截）；正常情况返回 `(expire_at_ms - now) / 1000`。

**移除过期时间**的 PERSIST 命令也是写操作。apply 层将 `expire_ms` 设为 `REDIS_NO_EXPIRE`（0），重新编码写回。如果 key 不存在或没有设置过期时间，返回 0；成功移除返回 1。

## 关于 Compaction Filter

原始文档中提到了第三层——通过 RocksDB Compaction Filter 在 compaction 时清理过期数据和孤儿子键。这个机制目前在规划中，尚未实现。当前的过期数据清理完全依赖前两层：被动过期保证读取正确性，TTL Cleaner 负责空间回收。

如果未来实现 Compaction Filter 层，需要注意几个问题。String 类型的值内联在 Metadata 中，Filter 可以直接判断和删除。但复合类型的子键（Hash field、Set member 等）分散在 DATA_CF 中，Filter 需要查询 Metadata CF 确认该子键的 version 是否仍然有效——这引入了跨 CF 的依赖，增加了实现复杂度。REDIS_ZSET_SCORE_CF 中的 score 索引也需要同步清理。这些跨 CF 的一致性问题，使得 Compaction Filter 的实现比看上去要困难得多。

## 检验你的理解

- 被动过期不删除数据。如果一个应用设置了大量短 TTL 的 key（比如每秒创建 10000 个 TTL=1 秒的 key），磁盘使用会怎样增长？TTL Cleaner 能跟上吗？
- TTL Cleaner 只在 Leader 上执行。如果 Leader 频繁切换（比如网络抖动导致反复选举），会影响清理效率吗？
- 被动过期使用 `butil::gettimeofday_ms()` 获取当前时间。如果集群中不同节点的时钟偏差较大（比如 5 秒），同一个 key 在不同节点上会不会出现"在 A 节点看到已过期、在 B 节点看到未过期"的情况？
- TTL Cleaner 的 `fill_cache = false` 避免污染 Block Cache。如果没有这个设置，一轮扫描 10 万个 key 会对正常读写性能产生多大影响？
- 如果要给 REDIS_METADATA_CF 实现 Compaction Filter 来清理过期的 String key，实现上有什么难点？（提示：考虑 Filter 的无状态特性——它只能看到当前 KV 对，不能查询其他 CF。）

---

> Part 4 到此结束。我们已经走完了从 Redis 客户端发出一条命令到 NeoKV 返回结果的完整链路：RESP 协议解析（brpc 内置）、命令分发（Handler 注册）、路由（CRC16 + SlotTable）、一致性检查（Leader / ReadIndex / MOVED）、Raft 写入或直接读取、以及过期管理（被动过期 + TTL Cleaner）。
>
> 接下来进入 [Part 5: 测试与运维](../5-测试与运维/16-测试体系.md)，了解 NeoKV 如何通过 Go 集成测试验证这些实现的正确性。
