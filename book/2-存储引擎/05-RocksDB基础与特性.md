# RocksDB 基础与特性

## 概览

- LSM-Tree 的基本原理：为什么 RocksDB 适合写密集型场景
- Column Family、Compaction 策略、Bloom Filter
- TransactionDB、WriteBatch、Compaction Filter、SST Ingestion
- Block Cache、压缩策略、Write Stall

这一章是 RocksDB 的"速查手册"，聚焦于 NeoKV 实际使用的特性。如果你已经熟悉 RocksDB，可以直接跳到 [06-存储架构设计](./06-存储架构设计.md)。

## 为什么选 RocksDB

在第一章我们讲过，NeoKV 用 Raft + RocksDB 替代了 Redis 的内存存储 + 异步复制。Raft 解决了一致性问题，RocksDB 则解决了持久性问题。但为什么偏偏选 RocksDB，而不是 LevelDB、BoltDB、或者直接用 B-Tree 引擎？

答案藏在 NeoKV 的工作负载特征里。NeoKV 的写入路径是 Raft 日志驱动的——每条 Redis 写命令都会变成一条 Raft 日志，committed 后 apply 到存储引擎。这意味着写入量远大于读取量（尤其在 Follower 上，几乎全是写入），而且写入模式是"批量顺序追加"（Raft 日志按 index 递增）。RocksDB 的底层数据结构 LSM-Tree 恰好就是为这种写密集场景设计的。

## LSM-Tree：写优化的核心

LSM-Tree（Log-Structured Merge-Tree）与传统的 B-Tree 有一个根本性的不同：B-Tree 直接在原地修改数据（in-place update），每次写入可能触发随机磁盘 I/O；LSM-Tree 则将所有写入转化为顺序追加（append-only），代价是读取时可能需要查找多个层级。

一次写入的完整旅程是这样的：数据首先被追加到 WAL（Write-Ahead Log），这是一个只追加的日志文件，保证持久性——即使进程崩溃，重启后可以从 WAL 恢复。然后数据写入内存中的 MemTable（一个基于 SkipList 的有序结构），此时就可以返回成功了。当 MemTable 积累到一定大小（NeoKV 中默认 128MB），它被冻结为 Immutable MemTable，然后由后台线程 Flush 为一个 SST 文件（Sorted String Table）写入磁盘的 L0 层。L0 层的 SST 文件之间可能有 key 范围重叠，后台的 Compaction 过程会将它们逐层合并——L1 及以上每层内的 key 范围互不重叠，越往下层数据越"旧"、文件越大。

```
Put(key, value)
    │
    ▼
┌──────────────┐
│  WAL         │  1. 顺序追加，保证持久性
└──────┬───────┘
       ▼
┌──────────────┐
│  MemTable    │  2. 写入内存 SkipList，即可返回
└──────┬───────┘
       │ 达到 128MB
       ▼
┌──────────────┐
│  Immutable   │  3. 冻结，后台 Flush
│  MemTable    │
└──────┬───────┘
       ▼
┌──────────────┐
│  L0 SST      │  4. SST 文件，key 范围可能重叠
└──────┬───────┘
       │ Compaction
       ▼
┌──────────────┐
│  L1 → L6     │  5. 逐层合并，每层内 key 不重叠
└──────────────┘
```

这个设计的关键特点是：写入只涉及 WAL（顺序写）和 MemTable（内存写），非常快；SST 文件一旦写入就不可变（immutable），简化了并发控制；Compaction 在后台异步执行，不阻塞前台读写。

读取一个 key 时，需要按"新到旧"的顺序查找：先查 MemTable，再查 Immutable MemTable，再查 L0（可能需要查多个文件），再逐层往下查 L1、L2……直到找到或确认不存在。这就是 LSM-Tree 的**读放大**问题。RocksDB 通过 Bloom Filter（快速判断 key 是否可能存在于某个 SST 文件中）和 Block Cache（缓存热数据块）来缓解。

LSM-Tree 的设计本质上是在三种放大之间做权衡。**写放大**是指实际写入磁盘的数据量与用户写入量的比值——Compaction 会导致数据被多次重写，写放大通常在 10-30 倍。**读放大**是指一次读取需要检查多少层——最坏情况下需要查每一层。**空间放大**是指旧版本数据在 Compaction 之前占用的额外空间。不同的 Compaction 策略在这三者之间做不同的取舍。

## Column Family

Column Family（CF）是 RocksDB 中逻辑隔离的 KV 命名空间。每个 CF 有独立的 MemTable、SST 文件、Compaction 配置和压缩策略，但所有 CF 共享同一个 WAL。共享 WAL 带来了一个重要的好处：跨 CF 的 WriteBatch 是原子的——要么全部成功，要么全部失败。

NeoKV 使用 8 个 Column Family，每个 CF 针对其数据特点做了专门的调优。为什么不用一个 CF 装所有数据？因为不同数据有截然不同的访问模式。Raft 日志是追加写入、很少读取的；Redis 数据是随机读写的；Binlog 是只关心最近数据、旧数据可以丢弃的。把它们放在同一个 CF 里，Compaction 策略无法同时满足所有模式。独立的 CF 让每种数据都能获得最适合自己的配置。

## Compaction 策略

Compaction 是 LSM-Tree 的核心后台操作，将多层 SST 文件合并，清理旧版本数据。RocksDB 提供三种主要策略，NeoKV 使用了其中两种。

**Level Compaction** 是最常用的策略。每层的容量是上一层的 10 倍（默认），当某层的数据量超过限制时，RocksDB 选择一个 SST 文件与下一层重叠的文件合并。这种策略的写放大较高（数据可能被多次合并），但读放大和空间放大较低。NeoKV 的 DATA_CF、REDIS_METADATA_CF、REDIS_ZSET_SCORE_CF 都使用这种策略。

**FIFO Compaction** 是最简单的策略——只保留最近的数据，当总大小超过限制时直接删除最旧的 SST 文件。这非常适合日志类数据，NeoKV 的 BIN_LOG_CF 使用这种策略，配置了 100GB 的上限。

## Bloom Filter

Bloom Filter 是 RocksDB 缓解读放大的关键武器。它是一种概率性数据结构，可以快速判断一个 key 是否**可能存在**于某个 SST 文件中。如果 Bloom Filter 说"不存在"，那一定不存在（无假阴性）；如果说"可能存在"，那不一定存在（有假阳性）。通过 Bloom Filter，RocksDB 可以跳过大量不包含目标 key 的 SST 文件，大幅减少不必要的磁盘读取。

RocksDB 还支持基于 key 前缀的 Bloom Filter。通过 `FixedPrefixTransform(n)` 配置，只用 key 的前 n 个字节构建 Bloom Filter。这在 NeoKV 中非常有用——NeoKV 所有 key 都以 `[region_id:8][index_id:8]` 作为前缀，配置 16 字节的前缀 Bloom Filter 后，查询某个 Region 的数据时可以快速排除不相关的 SST 文件。

RocksDB 提供两种 Filter 实现：传统的 Bloom Filter（10 bits/key）和更新的 Ribbon Filter（约 9.9 bits/key，更省空间但构建和查询略慢）。NeoKV 默认使用 Bloom Filter，可通过配置切换到 Ribbon Filter。

## TransactionDB 与 WriteBatch

NeoKV 使用 `rocksdb::TransactionDB` 而非普通的 `rocksdb::DB`。TransactionDB 在 DB 之上提供了悲观事务支持——先获取行锁，再执行操作，最后原子提交。但在 NeoKV 的 Redis 场景中，我们更多使用的是 WriteBatch：

```cpp
rocksdb::WriteBatch batch;
batch.Put(cf1, key1, value1);
batch.Put(cf2, key2, value2);
batch.Delete(cf1, key3);
db->Write(write_options, &batch);
```

WriteBatch 保证一组操作要么全部成功，要么全部失败。这对 Redis 的复合操作至关重要——比如 HSET 需要同时更新 metadata CF（size +1）和 data CF（写入 field），这两个操作必须原子执行。如果只有一个成功了另一个失败了，数据就不一致了。

## Compaction Filter

Compaction Filter 允许在 compaction 过程中自定义数据清理逻辑。当 RocksDB 合并 SST 文件时，会对每个 KV 对调用你注册的 Filter 函数——返回 true 表示删除这个 KV，返回 false 表示保留。

NeoKV 使用了两个 Compaction Filter。`SplitCompactionFilter` 作用于 DATA_CF，在 Region Split 后清理超出新范围的数据。`RaftLogCompactionFilter` 作用于 RAFT_LOG_CF（通过自定义 Raft 日志存储注册），清理已截断的 Raft 日志——这就是我们在第三章讨论过的惰性删除机制。

Compaction Filter 的优势在于**零额外 I/O**——数据清理与 compaction 合并执行，不需要单独发起删除操作。

## SST Ingestion

`IngestExternalFile` 允许将预排序的 SST 文件直接导入 RocksDB，跳过 MemTable 和 WAL。导入的 SST 文件被直接放入 LSM-Tree 的合适层级，避免了逐条写入的写放大，而且整个操作是原子的。

NeoKV 在 Raft 快照加载中使用了这个特性——第三章中我们讨论的 `SstWriterAdaptor`，Follower 收到 Leader 的快照数据后，写入临时 SST 文件再通过 `IngestExternalFile` 导入 RocksDB。这比逐条 Put 快了一个数量级。

## Block Cache、压缩与 Write Stall

**Block Cache** 是 RocksDB 读性能的关键。RocksDB 从 SST 文件中读取数据时，以 Block 为单位（NeoKV 配置为 64KB），读出后缓存在内存中。NeoKV 配置了 8GB 的 LRU Cache（8 个 shard 以减少锁竞争），热数据命中缓存后不需要访问磁盘。

**压缩**方面，NeoKV 对不同层级使用不同策略。L0 不使用压缩——L0 文件生命周期短，很快会被 compaction，压缩/解压的 CPU 开销得不偿失。L1 到 L6 使用 LZ4 压缩——快速压缩/解压，CPU 开销低。最底层可选启用 ZSTD + 字典压缩——压缩率更高，适合冷数据。

**Write Stall** 是 RocksDB 的自我保护机制。当 L0 文件数量或 pending compaction 数据量超过阈值时，RocksDB 会主动减慢甚至暂停写入，防止 Compaction 跟不上写入速度导致系统雪崩。在 NeoKV 中，默认配置是 L0 文件数达到 10 时减慢写入（slowdown），达到 40 时暂停写入（stop）；pending compaction 达到 64GB 时减慢，256GB 时暂停。Write Stall 会直接影响 Raft apply 的速度，进而影响写入延迟，是运维监控的重要指标。

## 检验你的理解

- 为什么 RocksDB 适合写密集型场景？它的读性能瓶颈在哪里？
- Column Family 共享 WAL 有什么好处？如果某个 CF 的写入量远大于其他 CF，会有什么影响？
- Prefix Bloom Filter 的前缀长度如何影响过滤效果？前缀太短或太长分别有什么问题？
- WriteBatch 和 Transaction 的区别是什么？NeoKV 为什么主要使用 WriteBatch 而不是 Transaction？
- 为什么 L0 不使用压缩？如果 L0 也使用 LZ4 压缩，会有什么问题？

## 延伸阅读

- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki) — 官方文档，非常详尽
- [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) — 性能调优指南
- [LSM-Tree 论文](https://www.cs.umb.edu/~poneil/lsmtree.pdf) — 原始论文（1996）

---

> 下一章：[06-存储架构设计](./06-存储架构设计.md) — 我们将深入 NeoKV 的 RocksWrapper，了解 8 个 Column Family 的具体设计和 Key 编码体系。
