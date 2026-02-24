# RocksDB 基础与特性

## 本章概览

在这一章中，我们将了解：

- LSM-Tree 的基本原理：为什么 RocksDB 适合写密集型场景
- Column Family：逻辑隔离的 KV 命名空间
- Compaction 策略：Level、Universal、FIFO 的区别与选择
- Bloom Filter：如何加速点查询
- TransactionDB：悲观事务与 WriteBatch
- Compaction Filter：在 compaction 过程中自定义数据清理
- SST Ingestion：高效的批量数据导入

这一章是 RocksDB 的"速查手册"，聚焦于 NeoKV 实际使用的特性。如果你已经熟悉 RocksDB，可以直接跳到 [06-存储架构设计](./06-存储架构设计.md)。

## 1. LSM-Tree：写优化的存储结构

RocksDB 的核心数据结构是 **LSM-Tree（Log-Structured Merge-Tree）**。与 B-Tree（MySQL InnoDB）不同，LSM-Tree 将随机写转化为顺序写，代价是读取时可能需要查找多个层级。

### 1.1 写入流程

```
Client: Put(key, value)
    │
    ▼
┌──────────────┐
│  Write-Ahead │  1. 先写 WAL（顺序追加，保证持久性）
│  Log (WAL)   │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  MemTable    │  2. 写入内存中的有序结构（SkipList）
│  (可变)      │     此时即可返回成功
└──────┬───────┘
       │ 达到大小限制
       ▼
┌──────────────┐
│  Immutable   │  3. 冻结为不可变 MemTable
│  MemTable    │
└──────┬───────┘
       │ Flush（后台）
       ▼
┌──────────────┐
│  L0 SST      │  4. 刷写为 SST 文件（Sorted String Table）
│  文件        │     L0 的 SST 之间可能有 key 范围重叠
└──────┬───────┘
       │ Compaction（后台）
       ▼
┌──────────────┐
│  L1 SST      │  5. Compaction 合并 SST 文件
├──────────────┤     L1+ 的每层内 key 范围不重叠
│  L2 SST      │     越往下层，数据越"旧"，文件越大
├──────────────┤
│  ...         │
└──────────────┘
```

关键特点：
- **写入只涉及 WAL（顺序写）和 MemTable（内存写）**，非常快
- **SST 文件一旦写入就不可变**（immutable），简化了并发控制
- **Compaction 在后台异步执行**，不阻塞前台读写

### 1.2 读取流程

读取一个 key 时，需要按"新到旧"的顺序查找：

```
Get(key)
    │
    ├─ 1. 查 MemTable（最新数据）
    │
    ├─ 2. 查 Immutable MemTable
    │
    ├─ 3. 查 L0 SST（可能需要查多个文件）
    │
    ├─ 4. 查 L1 SST（二分查找定位文件，再在文件内查找）
    │
    ├─ 5. 查 L2 SST
    │
    └─ ... 直到找到或确认不存在
```

这就是 LSM-Tree 的**读放大**问题：最坏情况下需要查找每一层。RocksDB 通过 Bloom Filter、Block Cache 等机制来缓解。

### 1.3 三种放大

LSM-Tree 的设计涉及三种放大的权衡：

| 放大类型 | 含义 | LSM-Tree 表现 |
|---------|------|--------------|
| 写放大 | 实际写入磁盘的数据量 / 用户写入的数据量 | 中等（Compaction 导致数据被多次重写） |
| 读放大 | 实际读取磁盘的数据量 / 用户请求的数据量 | 较高（可能查找多层） |
| 空间放大 | 实际占用的磁盘空间 / 有效数据量 | 中等（旧版本数据在 Compaction 前占用空间） |

不同的 Compaction 策略在这三者之间做不同的取舍。

## 2. Column Family

Column Family（CF）是 RocksDB 中逻辑隔离的 KV 命名空间。每个 CF 有独立的：

- MemTable 和 SST 文件
- Compaction 配置
- 压缩策略
- Bloom Filter 配置

但所有 CF **共享同一个 WAL**。这意味着跨 CF 的 WriteBatch 是原子的。

```
┌─────────────────────────────────────────────┐
│                 RocksDB 实例                  │
│                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐    │
│  │  CF: data │ │CF: meta  │ │CF: raft  │    │
│  │           │ │          │ │  _log    │    │
│  │ MemTable  │ │ MemTable │ │ MemTable │    │
│  │ L0 SSTs   │ │ L0 SSTs  │ │ L0 SSTs  │    │
│  │ L1 SSTs   │ │ L1 SSTs  │ │ L1 SSTs  │    │
│  │ ...       │ │ ...      │ │ ...      │    │
│  └──────────┘ └──────────┘ └──────────┘    │
│                                              │
│  ┌──────────────────────────────────────┐    │
│  │          共享 WAL                     │    │
│  └──────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

NeoKV 使用 8 个 CF，每个 CF 针对其数据特点做了专门的调优。我们将在下一章详细讨论。

为什么使用多个 CF 而不是一个？
- **不同数据有不同的访问模式**：Raft 日志是追加写入、顺序读取；Redis 数据是随机读写
- **独立的 Compaction**：Raft 日志的 Compaction 不会影响 Redis 数据的读写性能
- **独立的 Bloom Filter**：每个 CF 可以配置不同的前缀长度

## 3. Compaction 策略

Compaction 是 LSM-Tree 的核心后台操作，将多层 SST 文件合并，清理旧版本数据。RocksDB 提供三种主要策略：

### 3.1 Level Compaction

```
L0:  [SST] [SST] [SST]     ← key 范围可能重叠
      │     │     │
      ▼     ▼     ▼
L1:  [  SST  ] [  SST  ]   ← 每层内 key 范围不重叠
              │
              ▼
L2:  [SST] [SST] [SST] [SST]  ← 容量是上一层的 10 倍
```

- 每层容量是上一层的 10 倍（默认）
- L0 → L1 的 compaction 可能涉及 L1 的所有文件（因为 L0 文件 key 范围重叠）
- **写放大较高，但读放大和空间放大较低**
- NeoKV 的 DATA_CF、REDIS_METADATA_CF 使用此策略

### 3.2 FIFO Compaction

```
[SST_newest] [SST_older] [SST_oldest] → 超过大小限制时，直接删除最旧的文件
```

- 最简单的策略：只保留最近的数据，旧数据直接丢弃
- 适合日志类数据（只关心最近的数据）
- NeoKV 的 BIN_LOG_CF 使用此策略（100GB 上限）

### 3.3 Universal Compaction

- 类似 Level Compaction，但更灵活
- 适合写入量极大、希望减少写放大的场景
- NeoKV 当前未使用

## 4. Bloom Filter

Bloom Filter 是一种概率性数据结构，用于快速判断一个 key **是否可能存在**于某个 SST 文件中：

- 如果 Bloom Filter 说"不存在"，那一定不存在（**无假阴性**）
- 如果 Bloom Filter 说"可能存在"，那不一定存在（**有假阳性**）

```
Get(key)
    │
    ├─ 查 SST 文件的 Bloom Filter
    │     │
    │     ├─ "不存在" → 跳过这个文件（节省一次磁盘读取）
    │     │
    │     └─ "可能存在" → 读取文件中的 Block 查找
```

### 4.1 Prefix Bloom Filter

RocksDB 支持基于 key 前缀的 Bloom Filter。通过 `FixedPrefixTransform(n)` 配置，只用 key 的前 n 个字节构建 Bloom Filter。

这在 NeoKV 中非常有用：

```
Key: [region_id:8][index_id:8][slot:2][user_key_len:4][user_key]
     |<---- 16 字节前缀 ---->|

Prefix Bloom Filter 使用前 16 字节
→ 同一个 Region + Table 的所有 key 共享一个 Bloom Filter 条目
→ 查询时先用前缀过滤，快速排除不相关的 SST 文件
```

### 4.2 Bloom vs Ribbon

RocksDB 提供两种 Filter 实现：

| 类型 | 空间效率 | 构建速度 | 查询速度 |
|------|---------|---------|---------|
| Bloom Filter | 10 bits/key | 快 | 快 |
| Ribbon Filter | 9.9 bits/key（更省空间） | 较慢 | 略慢 |

NeoKV 默认使用 Bloom Filter（10 bits/key），可通过配置切换到 Ribbon Filter。

## 5. TransactionDB

NeoKV 使用 `rocksdb::TransactionDB` 而非普通的 `rocksdb::DB`。TransactionDB 在 DB 之上提供了悲观事务支持：

```cpp
// 悲观事务：先获取行锁，再执行操作
auto* txn = txn_db->BeginTransaction(write_options);
txn->Put("key1", "value1");  // 获取 key1 的行锁
txn->Put("key2", "value2");  // 获取 key2 的行锁
txn->Commit();                // 原子提交，释放锁
```

但在 NeoKV 的 Redis 场景中，我们更多使用的是 **WriteBatch**：

```cpp
// WriteBatch：一组操作原子执行（不需要行锁）
rocksdb::WriteBatch batch;
batch.Put(cf1, key1, value1);
batch.Put(cf2, key2, value2);
batch.Delete(cf1, key3);
db->Write(write_options, &batch);  // 原子写入
```

WriteBatch 保证一组操作要么全部成功，要么全部失败。这对 Redis 的复合操作非常重要——比如 HSET 需要同时更新 metadata CF（size +1）和 data CF（写入 field），这两个操作必须原子执行。

## 6. Compaction Filter

Compaction Filter 允许在 compaction 过程中自定义数据过滤逻辑。当 RocksDB 合并 SST 文件时，会对每个 KV 对调用 Filter 函数：

```cpp
class MyCompactionFilter : public rocksdb::CompactionFilter {
    bool Filter(int level,
                const rocksdb::Slice& key,
                const rocksdb::Slice& value,
                std::string* new_value,
                bool* value_changed) const override {
        // 返回 true → 删除这个 KV 对
        // 返回 false → 保留
        // 设置 new_value + value_changed → 修改 value
    }
};
```

NeoKV 使用了两个 Compaction Filter：

| Filter | CF | 用途 |
|--------|-----|------|
| `SplitCompactionFilter` | DATA_CF | Region Split 后清理超出范围的数据 |
| `RaftLogCompactionFilter` | RAFT_LOG_CF | 清理已截断的 Raft 日志 |

Compaction Filter 的优势在于**零额外 I/O**——数据清理与 compaction 合并执行，不需要单独的删除操作。

## 7. SST Ingestion

`IngestExternalFile` 允许将预排序的 SST 文件直接导入 RocksDB，跳过 MemTable 和 WAL：

```cpp
// 1. 创建 SST 文件
rocksdb::SstFileWriter writer(env_options, options);
writer.Open("/tmp/data.sst");
writer.Put("key1", "value1");  // 必须按 key 排序
writer.Put("key2", "value2");
writer.Finish();

// 2. 导入 RocksDB
rocksdb::IngestExternalFileOptions ingest_options;
ingest_options.move_files = true;  // 移动而非复制
db->IngestExternalFile({"/tmp/data.sst"}, ingest_options);
```

NeoKV 在两个场景使用 SST Ingestion：
- **Raft 快照加载**：Follower 收到 Leader 的快照数据后，写入 SST 文件再导入（见第 2 章）
- **冷数据导入**：从远程文件系统导入冷数据

SST Ingestion 的优势：
- **跳过 WAL**：不产生 WAL 写入，减少写放大
- **跳过 MemTable**：直接放入 LSM-Tree 的合适层级
- **原子性**：导入操作是原子的，要么完全成功，要么完全失败

## 8. 其他重要特性

### 8.1 Block Cache

RocksDB 使用 Block Cache 缓存从 SST 文件读取的数据块（默认 4KB-64KB）。NeoKV 配置了 8GB 的 LRU Cache：

```cpp
auto cache = rocksdb::NewLRUCache(8ULL * 1024 * 1024 * 1024,  // 8GB
                                   8);  // 8 个 shard，减少锁竞争
```

Block Cache 是 RocksDB 读性能的关键——热数据命中缓存后，读取不需要访问磁盘。

### 8.2 压缩

NeoKV 对不同层级使用不同的压缩算法：

| 层级 | 压缩算法 | 原因 |
|------|---------|------|
| L0 | 无压缩 | L0 文件生命周期短，很快会被 compaction |
| L1-L6 | LZ4 | 快速压缩/解压，CPU 开销低 |
| 最底层（可选） | ZSTD + 字典 | 压缩率更高，适合冷数据 |

### 8.3 Write Stall

当 L0 文件数量或 pending compaction 数据量超过阈值时，RocksDB 会减慢甚至暂停写入，防止系统过载：

| 条件 | 行为 |
|------|------|
| L0 文件数 ≥ 10 | 减慢写入（slowdown） |
| L0 文件数 ≥ 40 | 暂停写入（stop） |
| Pending compaction ≥ 64GB | 减慢写入 |
| Pending compaction ≥ 256GB | 暂停写入 |

Write Stall 是 RocksDB 的自我保护机制。在 NeoKV 中，Write Stall 会导致 Raft apply 变慢，进而影响写入延迟。监控 Write Stall 是运维的重要指标。

## 检验你的理解

- 为什么 RocksDB 适合写密集型场景？它的读性能瓶颈在哪里？
- Column Family 共享 WAL 有什么好处？如果某个 CF 的写入量远大于其他 CF，会有什么影响？
- Prefix Bloom Filter 的前缀长度如何影响过滤效果？前缀太短或太长分别有什么问题？
- WriteBatch 和 Transaction 的区别是什么？NeoKV 为什么主要使用 WriteBatch 而不是 Transaction？
- Compaction Filter 在 compaction 时执行，如果 Filter 函数很慢（比如需要查询其他 CF），会有什么影响？
- 为什么 L0 不使用压缩？如果 L0 也使用 LZ4 压缩，会有什么问题？

## 延伸阅读

- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki) — 官方文档，非常详尽
- [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) — 性能调优指南
- [LSM-Tree 论文](https://www.cs.umb.edu/~poneil/lsmtree.pdf) — 原始论文（1996）

---

> 下一章：[06-存储架构设计](./06-存储架构设计.md) — 我们将深入 NeoKV 的 RocksWrapper，了解 8 个 Column Family 的具体设计和 Key 编码体系。
