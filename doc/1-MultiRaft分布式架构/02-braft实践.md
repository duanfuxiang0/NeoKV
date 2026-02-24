# braft 实践

## 本章概览

在这一章中，我们将了解：

- braft 框架的核心抽象和使用方式
- 在 Multi-Raft 场景下直接使用 braft 会遇到哪些实际问题
- NeoKV 如何通过自定义 Raft Log Storage、Meta Storage 和 Snapshot Adaptor 解决这些问题
- Raft 日志的惰性删除机制

braft 是百度开源的 C++ Raft 实现，基于 brpc 框架。它提供了完整的 Raft 协议实现，包括 Leader 选举、日志复制、快照、成员变更等。NeoKV 的分布式能力完全建立在 braft 之上，但我们对它做了几项关键定制。

## 1. braft 的核心抽象

braft 的设计围绕几个核心接口：

```
┌─────────────────────────────────────────────────┐
│                  braft::Node                     │
│                                                  │
│  ┌─────────────┐  ┌──────────────────────────┐  │
│  │  Raft 协议   │  │     可插拔存储后端         │  │
│  │  选举/复制   │  │                          │  │
│  │             │  │  LogStorage (日志存储)     │  │
│  │             │  │  RaftMetaStorage (元数据)  │  │
│  │             │  │  SnapshotAdaptor (快照)    │  │
│  └─────────────┘  └──────────────────────────┘  │
│         │                                        │
│         ▼                                        │
│  ┌─────────────┐                                │
│  │ StateMachine │  ← 用户实现                    │
│  │  on_apply()  │                                │
│  │  on_snapshot │                                │
│  └─────────────┘                                │
└─────────────────────────────────────────────────┘
```

### 1.1 StateMachine（状态机）

这是用户需要实现的核心接口。braft 保证所有节点以相同的顺序执行 `on_apply()`，从而保证状态一致：

```cpp
class StateMachine {
    // 日志被 committed 后调用，在此执行实际的业务逻辑
    virtual void on_apply(braft::Iterator& iter) = 0;

    // 保存快照（用于日志截断和新节点追赶）
    virtual void on_snapshot_save(braft::SnapshotWriter* writer,
                                  braft::Closure* done) = 0;

    // 加载快照
    virtual int on_snapshot_load(braft::SnapshotReader* reader) = 0;

    // Leader 状态变更通知
    virtual void on_leader_start(int64_t term) = 0;
    virtual void on_leader_stop(const braft::LeaderChangeContext& ctx) = 0;
};
```

在 NeoKV 中，`Region` 类继承了 `braft::StateMachine`（见 `include/store/region.h`）。每个 Region 就是一个 Raft 状态机。

### 1.2 LogStorage（日志存储）

braft 通过 URI 机制支持可插拔的日志存储后端：

```cpp
// braft 默认实现：基于本地文件系统
// 每个 Raft 组一个独立目录，日志写入 segment 文件
braft::Node node;
braft::NodeOptions options;
options.log_uri = "local://./raft_log/region_1";  // 默认文件存储
```

### 1.3 SnapshotAdaptor（快照适配器）

braft 的快照机制基于文件：Leader 将状态保存为一组文件，通过网络传输给 Follower。`FileSystemAdaptor` 接口允许自定义文件的读写方式。

## 2. Multi-Raft 的实际挑战

当我们在一个 Store 节点上运行数百个 Raft 组时，braft 的默认实现会遇到严重的工程问题。

### 2.1 问题一：日志存储的 I/O 放大

braft 默认为每个 Raft 组创建独立的日志目录和文件：

```
raft_data/
├── region_1/
│   ├── log_meta        # 日志元信息
│   ├── log_000001      # segment 文件
│   └── log_000002
├── region_2/
│   ├── log_meta
│   └── log_000001
├── region_3/
│   └── ...
└── ... (数百个目录)
```

问题：
- **随机 I/O**：数百个 Raft 组同时写日志，每个写不同的文件，磁盘寻道严重
- **文件描述符**：每个 Raft 组至少打开 2-3 个文件，数百个组就是上千个 fd
- **WAL 冗余**：每个 Raft 组独立 fsync，无法合并刷盘

### 2.2 问题二：快照与共享存储的矛盾

NeoKV 的所有 Region 共享一个 RocksDB 实例。但 braft 的快照机制假设每个 Raft 组有独立的数据文件：

```
// braft 期望的快照模型：
snapshot/
├── data_file_1
├── data_file_2
└── __raft_snapshot_meta

// NeoKV 的实际情况：
// 所有 Region 的数据混在同一个 RocksDB 中
// 无法简单地"复制文件"来生成某个 Region 的快照
```

### 2.3 问题三：日志截断的性能

Raft 需要定期截断已应用的旧日志（log compaction）。braft 默认通过删除旧的 segment 文件实现。但在共享 RocksDB 的方案中，日志条目是 KV 对，逐条删除会产生大量的 delete tombstone。

## 3. NeoKV 的解决方案

NeoKV 通过 braft 的 URI 扩展机制，注册了三个自定义存储后端：

```cpp
// src/raft/my_raft_log.cpp
// 通过 pthread_once 保证只注册一次
static void register_once() {
    // 注册自定义日志存储
    braft::log_storage_extension()->RegisterOrDie("myraftlog", &raft_log_instance);
    // 注册自定义元数据存储
    braft::meta_storage_extension()->RegisterOrDie("myraftmeta", &raft_meta_instance);
}
```

使用时通过 URI 前缀激活：

```cpp
options.log_uri  = "myraftlog://my_raft_log?id=" + std::to_string(region_id);
options.raft_meta_uri = "myraftmeta://my_raft_meta?id=" + std::to_string(region_id);
```

### 3.1 自定义 Raft Log Storage

**核心思路**：所有 Raft 组的日志共享一个 RocksDB 的 `RAFT_LOG_CF` Column Family。

**关键文件**：
- `include/raft/my_raft_log_storage.h` — 类定义和 key 格式常量
- `src/raft/my_raft_log_storage.cpp` — 完整实现

#### Key 格式

RAFT_LOG_CF 中存储三种类型的数据，通过 key 中的类型字节区分：

```
┌─────────────────────────────────────────────────────────┐
│ Log Meta (首条日志索引)                                   │
│ Key:  [region_id : 8B] [0x01]                           │
│ Value: [first_log_index : 8B]                           │
├─────────────────────────────────────────────────────────┤
│ Log Data (日志条目)                                      │
│ Key:  [region_id : 8B] [0x02] [log_index : 8B]         │
│ Value: [LogHead] [serialized_data]                      │
├─────────────────────────────────────────────────────────┤
│ Raft Meta (Term 和 VotedFor)                            │
│ Key:  [region_id : 8B] [0x03]                           │
│ Value: [StablePBMeta protobuf]                          │
└─────────────────────────────────────────────────────────┘
```

其中 `region_id` 使用大端编码，保证同一 Region 的所有日志在 RocksDB 中物理相邻。类型字节 `0x01 < 0x02 < 0x03` 的顺序保证 Log Meta 排在 Log Data 之前。

`LogHead` 结构包含日志条目的元信息：

```cpp
// include/raft/my_raft_log_storage.h
struct LogHead {
    int64_t term;           // Raft Term
    int type;               // 日志类型（数据/配置变更/空操作）
    int data_len;           // 数据长度
    int peers_cnt;          // 节点列表长度
    int old_peers_cnt;      // 旧节点列表长度（成员变更时使用）
};
```

#### 初始化流程

当一个 Region 的 Raft 组启动时，`MyRaftLogStorage::init()` 执行以下步骤：

1. 从 URI 中解析 `region_id`
2. 读取 Log Meta key（`[region_id][0x01]`）获取 `first_log_index`
3. 从 `first_log_index` 开始正向扫描所有 Log Data，构建内存中的 `IndexTermMap`（index → term 的映射）
4. 确定 `last_log_index`

```cpp
// src/raft/my_raft_log_storage.cpp — init() 简化逻辑
int MyRaftLogStorage::init(braft::ConfigurationManager* config_mgr) {
    // 1. 读取 first_log_index
    _first_log_index = read_log_meta();

    // 2. 扫描所有日志条目，构建 IndexTermMap
    auto iter = _db->new_iterator(RAFT_LOG_CF);
    std::string prefix = encode_log_data_prefix(_region_id);
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        // 解析 LogHead，记录 (index, term)
        _index_term_map.append(index, head.term);
        _last_log_index = index;
    }
    return 0;
}
```

#### 日志追加

`append_entries()` 将一批日志条目写入 RocksDB：

```cpp
int MyRaftLogStorage::append_entries(const std::vector<braft::LogEntry*>& entries,
                                     IOMetric* metric) {
    rocksdb::WriteBatch batch;
    for (auto* entry : entries) {
        // 编码 key: [region_id][0x02][index]
        // 编码 value: [LogHead][serialized_data]
        batch.Put(cf_handle, key, value);
        _index_term_map.append(entry->id.index, entry->id.term);
    }
    _db->write(write_options, &batch);
    _last_log_index = entries.back()->id.index;
    return entries.size();
}
```

所有 Raft 组的日志追加最终都变成对同一个 RocksDB 的 WriteBatch 操作。RocksDB 内部会将多个并发写入合并到同一次 WAL fsync 中（group commit），大幅减少磁盘 I/O。

#### 日志截断

Raft 需要截断已应用的旧日志。NeoKV 采用**惰性删除**策略：

```cpp
int MyRaftLogStorage::truncate_prefix(int64_t first_index_kept) {
    // 只更新内存中的 first_log_index 和持久化的 Log Meta
    _first_log_index = first_index_kept;
    save_log_meta(first_index_kept);
    _index_term_map.truncate_prefix(first_index_kept);
    // 注意：并不立即删除旧的日志条目！
    return 0;
}
```

旧日志条目的实际删除由 `RaftLogCompactionFilter` 在 RocksDB compaction 时完成：

```cpp
// include/raft/raft_log_compaction_filter.h
bool Filter(int level, const Slice& key, const Slice& value,
            std::string* new_value, bool* value_changed) const {
    // 提取 region_id 和 log_index
    int64_t region_id = decode_region_id(key);
    int64_t index = decode_log_index(key);

    // 查找该 Region 的 first_log_index
    int64_t first_index = _first_index_map[region_id];

    // 同时检查 split 所需的日志（不能删除正在 split 的日志）
    int64_t split_index = SplitIndexGetter::get_instance()->get(region_id);

    // 如果日志已过期且不被 split 需要，则删除
    return index < std::min(first_index, split_index);
}
```

这种惰性删除的优势：
- **截断操作是 O(1)**：只更新一个 meta key，不需要遍历删除
- **与 compaction 合并**：旧日志在 compaction 时顺便清理，不产生额外 I/O
- **Split 感知**：正在进行 Region Split 时，需要回放的日志不会被误删

### 3.2 自定义 Raft Meta Storage

Raft 需要持久化两个关键元数据：当前 Term 和 VotedFor（本 Term 投票给了谁）。这两个值必须在节点重启后恢复，否则可能违反 Raft 的安全性。

**关键文件**：
- `include/raft/my_raft_meta_storage.h`
- `src/raft/my_raft_meta_storage.cpp`

NeoKV 将这些元数据存储在 RAFT_LOG_CF 中，与日志条目共享同一个 CF：

```cpp
// Key: [region_id : 8B] [0x03]
// Value: StablePBMeta protobuf { term, votedfor }

butil::Status MyRaftMetaStorage::set_term_and_votedfor(
        int64_t term, const braft::PeerId& peer_id, ...) {
    StablePBMeta meta;
    meta.set_term(term);
    meta.set_votedfor(peer_id.to_string());
    // 写入 RocksDB，使用 sync write 保证持久化
    return _db->put(write_options, cf_handle, key, meta.SerializeAsString());
}
```

为什么不用独立文件？因为在 Multi-Raft 场景下，数百个 Raft 组各自 fsync 一个小文件的开销远大于共享 RocksDB 的 WAL 合并写入。

### 3.3 自定义 Snapshot Adaptor

这是最复杂的定制。问题的核心是：**所有 Region 共享一个 RocksDB，如何为单个 Region 生成快照？**

**关键文件**：
- `include/raft/rocksdb_file_system_adaptor.h`
- `src/raft/rocksdb_file_system_adaptor.cpp`

#### 设计思路

NeoKV 通过 braft 的 `FileSystemAdaptor` 接口，将快照"伪装"成两个虚拟文件：

```
snapshot/
├── region_data_snapshot.sst    # 虚拟文件：Region 在 DATA_CF 中的数据
└── region_meta_snapshot.sst    # 虚拟文件：Region 在 METAINFO_CF 中的元数据
```

这两个文件并不真实存在于磁盘上。当 braft 尝试读取它们时，NeoKV 的自定义 Adaptor 会从 RocksDB 中实时流式读取对应 Region 的数据。

#### Leader 端：流式读取

当 Leader 需要发送快照给 Follower 时，`RocksdbReaderAdaptor` 被激活：

```cpp
// 简化的读取流程
ssize_t RocksdbReaderAdaptor::read(butil::IOPortal* portal,
                                    off_t offset, size_t size) {
    if (!_iter_context) {
        // 首次读取：创建 RocksDB Snapshot + Iterator
        auto* snapshot = _db->GetSnapshot();
        auto* iter = _db->NewIterator(read_options, cf_handle);

        // 定位到该 Region 的 key 范围起点
        iter->Seek(region_start_key);
    }

    // 流式读取：每次调用返回一批 KV 对
    while (iter->Valid() && bytes_read < size) {
        // 检查 key 是否仍在 Region 范围内
        if (key >= region_end_key) break;

        // 编码为 [key_len][key][value_len][value] 格式
        encode_kv_to_portal(portal, iter->key(), iter->value());
        iter->Next();
    }
    return bytes_read;
}
```

关键点：使用 **RocksDB Snapshot** 保证读取一致性。即使快照传输过程中有新的写入，Snapshot 保证读到的是创建时刻的一致视图。

#### Follower 端：SST 写入与导入

Follower 收到快照数据后，`SstWriterAdaptor` 将数据写入 SST 文件，然后通过 `IngestExternalFile` 导入 RocksDB：

```cpp
// 简化的写入流程
ssize_t SstWriterAdaptor::write(const butil::IOBuf& data) {
    // 解码 KV 对
    while (data.remaining() > 0) {
        auto [key, value] = decode_kv(data);
        _sst_writer->Put(key, value);  // 写入 SST 文件
    }
    return bytes_written;
}

bool SstWriterAdaptor::close() {
    _sst_writer->Finish();
    // 将 SST 文件直接导入 RocksDB（不经过 MemTable）
    _db->IngestExternalFile(cf_handle, {sst_path}, ingest_options);
    return true;
}
```

`IngestExternalFile` 是 RocksDB 提供的高效数据导入接口，它直接将预排序的 SST 文件放入 LSM-Tree 的合适层级，避免了逐条 Put 的写放大。

#### 快照流程总结

```
Leader                              Follower
  │                                    │
  │  1. 创建 RocksDB Snapshot          │
  │  2. 注册虚拟文件到 braft            │
  │                                    │
  │ ──── braft 快照传输协议 ──────────► │
  │      (读取虚拟文件内容)              │
  │                                    │
  │  RocksdbReaderAdaptor::read()      │  SstWriterAdaptor::write()
  │  从 Snapshot 流式读取 KV            │  写入临时 SST 文件
  │                                    │
  │ ──── 传输完成 ────────────────────► │
  │                                    │  3. SstWriterAdaptor::close()
  │                                    │     IngestExternalFile()
  │                                    │     SST 直接导入 RocksDB
  │  4. 释放 RocksDB Snapshot          │
```

## 4. IndexTermMap：内存中的日志索引

为了快速查询某个 log index 对应的 Term（这在选举和日志比较时频繁使用），NeoKV 维护了一个内存数据结构 `IndexTermMap`：

```cpp
// include/raft/index_term_map.h
class IndexTermMap {
    std::deque<std::pair<int64_t, int64_t>> _map;  // (index, term) 有序队列
    // ...
};
```

这是一个简单的有序 deque，支持：
- `append(index, term)`：追加新条目
- `get_term(index)`：二分查找获取 term
- `truncate_prefix(index)` / `truncate_suffix(index)`：截断

由于 Raft 日志是连续追加的，deque 的内存布局非常高效，且支持两端的快速截断。

## 5. 代码导读

| 文件 | 内容 |
|------|------|
| `include/raft/my_raft_log_storage.h` | MyRaftLogStorage 类定义、LogHead 结构、key 格式常量 |
| `src/raft/my_raft_log_storage.cpp` | 日志存储完整实现：init、append、truncate |
| `include/raft/my_raft_meta_storage.h` | MyRaftMetaStorage 类定义 |
| `src/raft/my_raft_meta_storage.cpp` | Term/VotedFor 持久化 |
| `src/raft/my_raft_log.cpp` | URI 注册（`myraftlog://`、`myraftmeta://`） |
| `include/raft/rocksdb_file_system_adaptor.h` | 快照适配器：RocksdbReaderAdaptor、SstWriterAdaptor |
| `src/raft/rocksdb_file_system_adaptor.cpp` | 快照流式传输完整实现 |
| `include/raft/raft_log_compaction_filter.h` | Raft 日志的惰性删除 Compaction Filter |
| `include/raft/index_term_map.h` | 内存中的 (index, term) 索引 |

## 检验你的理解

- 为什么把所有 Raft 组的日志放在同一个 RocksDB CF 中比每组独立文件更好？在什么场景下这个方案可能反而更差？
- 惰性日志删除（通过 Compaction Filter）相比立即删除有什么优缺点？如果 compaction 长时间不触发，会有什么影响？
- 快照传输时，Leader 使用 RocksDB Snapshot 保证一致性。如果快照传输耗时很长（比如 Region 数据量很大），这个 Snapshot 会不会影响 RocksDB 的 compaction？
- `IndexTermMap` 使用 deque 而不是 map，这是为什么？（提示：Raft 日志的 index 是连续的）
- braft 的 URI 扩展机制（`RegisterOrDie`）本质上是一种什么设计模式？它有什么好处？

---

> 下一章：[03-Multi-Raft 与 Region](./03-Multi-Raft与Region.md) — 我们将深入 Region 作为 Raft 状态机的完整生命周期，以及 Store 如何管理数百个 Region。
