# Multi-Raft 与 Region

## 概览

- 在 Multi-Raft 场景下直接使用 braft 会遇到哪些实际问题
- NeoKV 如何通过自定义存储后端将数百个 Raft 组汇聚到一个 RocksDB 中
- Region 作为数据分片、Raft 状态机和服务单元的三重角色
- 写入路径与读取路径的完整流程
- Store 如何管理数百个 Region 并保持高性能

这一章是 Part 1 的核心，连接了 Raft 共识（第 1-2 章）与上层 Redis 命令处理（Part 3-4）。

## 当 braft 遇上 Multi-Raft

上一章我们通过 Counter 示例学会了 braft 的基本用法。Counter 只有一个 Raft 组，一切都很简单。但 NeoKV 不是只跑一个 Raft 组——第一章我们讲过，NeoKV 把数据分成多个 Region，每个 Region 是一个独立的 Raft 组。一个 Store 节点上可能同时运行数百个 Raft 组。

这就好比 braft 给了你一把很好的锤子，但你现在需要同时钉几百颗钉子。锤子本身没问题，但你得想想怎么高效地组织这个过程。

我们在实际开发中遇到了三个绕不过去的问题。

**第一个是日志存储的 I/O 放大。** braft 默认为每个 Raft 组创建独立的日志目录和文件。如果你只跑一两个 Raft 组，这完全没问题。但想象一下你有 300 个 Region，每个 Region 都在不停地写日志——300 个目录、几百个文件同时被写入，磁盘的寻道开销会非常严重。更糟糕的是，每个 Raft 组独立 fsync，意味着你无法利用 group commit 来合并刷盘。文件描述符也是个问题，每个 Raft 组至少打开 2-3 个文件，300 个组就是近千个 fd。这让我想起早年做数据库运维时的一个教训：当你把一个"单实例表现良好"的组件乘以几百倍，很多原本不是问题的东西都会变成问题。

**第二个是快照与共享存储的矛盾。** NeoKV 的一个重要设计决策是所有 Region 共享一个 RocksDB 实例。这样做的好处很多——统一的 compaction、统一的内存管理、避免几百个 RocksDB 实例各自占用资源。但这和 braft 的快照机制产生了冲突。braft 的快照模型假设每个 Raft 组有独立的数据文件——你把文件打包发给 Follower，Follower 解包恢复，就完成了快照传输。但在 NeoKV 里，所有 Region 的数据混在同一个 RocksDB 中，你没法简单地"复制文件"来生成某个 Region 的快照。

**第三个是日志截断的性能。** Raft 需要定期截断已经应用过的旧日志（log compaction）。braft 默认通过删除旧的 segment 文件来实现，干净利落。但如果我们把日志存到 RocksDB 里（后面你会看到我们确实这么做了），逐条删除日志条目会产生大量的 delete tombstone，反而给 RocksDB 的 compaction 增加负担。

这三个问题不是理论上的担忧，而是我们在原型阶段实际观测到的性能瓶颈。好在上一章我们讲过，braft 的存储层是可插拔的——这给了我们解决问题的空间。

## 共享一切

NeoKV 的核心思路可以用一句话概括：把所有 Raft 组的存储需求汇聚到同一个 RocksDB 实例中，利用 RocksDB 自身的 group commit 和 compaction 机制来消化 Multi-Raft 带来的 I/O 压力。

我们通过 braft 的 URI 扩展机制，注册了自定义的日志存储和元数据存储后端（`src/raft/my_raft_log.cpp`）：

```cpp
static void register_once() {
    braft::log_storage_extension()->RegisterOrDie("myraftlog", &raft_log_instance);
    braft::meta_storage_extension()->RegisterOrDie("myraftmeta", &raft_meta_instance);
}
```

使用时只需要把 URI 前缀从 `local://` 换成我们自己的：

```cpp
options.log_uri  = "myraftlog://my_raft_log?id=" + std::to_string(region_id);
options.raft_meta_uri = "myraftmeta://my_raft_meta?id=" + std::to_string(region_id);
```

就这么简单。braft 不需要任何修改，它只认 URI 前缀。接下来我们逐一看三个自定义后端的设计。

**自定义 Raft Log Storage。** 这是改动最大的一块。核心思路是让所有 Raft 组的日志共享一个 RocksDB 的 `RAFT_LOG_CF` Column Family。要让几百个 Raft 组的日志共存在同一个 CF 里而互不干扰，key 的设计至关重要。我们在 RAFT_LOG_CF 中存储三种类型的数据，通过 key 中的类型字节区分：

```
┌──────────────────────────────────────────────────────────────┐
│ Log Meta (首条日志索引)                                        │
│ Key:   [region_id : 8B] [0x01]                               │
│ Value: [first_log_index : 8B]                                │
├──────────────────────────────────────────────────────────────┤
│ Log Data (日志条目)                                           │
│ Key:   [region_id : 8B] [0x02] [log_index : 8B]             │
│ Value: [LogHead] [serialized_data]                           │
├──────────────────────────────────────────────────────────────┤
│ Raft Meta (Term 和 VotedFor)                                 │
│ Key:   [region_id : 8B] [0x03]                               │
│ Value: [StablePBMeta protobuf]                               │
└──────────────────────────────────────────────────────────────┘
```

这里有两个设计细节值得说道。`region_id` 使用大端编码，这保证了同一 Region 的所有日志在 RocksDB 中物理相邻——RocksDB 的 SST 文件按 key 排序存储，相邻的 key 会落在同一个 data block 里，读取时可以利用 block cache 的局部性。类型字节 `0x01 < 0x02 < 0x03` 的顺序保证 Log Meta 排在 Log Data 之前，Raft Meta 排在最后，扫描日志时不会被元数据干扰。

每条日志的 value 头部是一个紧凑的 `LogHead` 结构，包含 term、日志类型、数据长度、节点列表等元信息。这避免了每次读取日志时都需要完整反序列化 protobuf。

当一个 Region 的 Raft 组启动时，`MyRaftLogStorage::init()` 需要从 RocksDB 中恢复这个 Region 的日志状态。流程很直观：先从 Log Meta key 读出 `first_log_index`，然后从这个位置开始正向扫描所有 Log Data，在内存中重建一个 index 到 term 的映射表（`IndexTermMap`），同时确定 `last_log_index`。因为 key 以 `region_id` 开头，RocksDB 的 Seek 可以精确定位到这个 Region 的第一条日志，不会扫到其他 Region 的数据。

日志追加是最频繁的操作。`append_entries()` 将一批日志条目编码成 KV 对，通过 WriteBatch 一次性写入 RocksDB。这里的关键在于：所有 Raft 组的日志追加最终都变成对同一个 RocksDB 的 WriteBatch 操作。RocksDB 内部会将多个并发写入合并到同一次 WAL fsync 中（group commit），大幅减少磁盘 I/O。还记得前面说的"300 个 Raft 组各自 fsync"的问题吗？现在它们共享一个 WAL，RocksDB 帮我们做了合并。

然后是日志截断——这是我们前面提到的第三个问题。NeoKV 采用了一个巧妙的**惰性删除**策略：截断操作本身不删除任何数据，只是更新 `first_log_index` 这个标记。那旧日志什么时候真正被删除？答案是在 RocksDB compaction 的时候。我们注册了一个 `RaftLogCompactionFilter`（`include/raft/raft_log_compaction_filter.h`），它在 compaction 过程中检查每条日志的 index 是否小于当前 `first_log_index`，如果是就丢弃。这种做法的好处很明显：截断操作是 O(1) 的——只更新一个 meta key，不需要遍历删除几千条日志；旧日志在 compaction 时顺便清理，不产生额外的 I/O 开销。而且它还有一个额外的好处：正在进行 Region Split 时，需要回放的日志不会被误删——`SplitIndexGetter` 会告诉 Filter 哪些日志还在被 split 流程使用。

**自定义 Raft Meta Storage。** 相比日志存储，元数据存储的改造要简单得多，但同样重要。Raft 需要持久化当前 Term 和 VotedFor，这两个值必须在节点重启后恢复，否则可能违反 Raft 的安全性——比如一个节点重启后忘记了自己已经投过票，又投了一次，就可能导致同一个 Term 出现两个 Leader。NeoKV 将这些元数据存储在 RAFT_LOG_CF 中，与日志条目共享同一个 CF，key 格式就是前面提到的 `[region_id][0x03]`。为什么不用独立文件？原因和日志存储一样：在 Multi-Raft 场景下，数百个 Raft 组各自 fsync 一个小文件的开销远大于共享 RocksDB 的 WAL 合并写入。

**自定义 Snapshot Adaptor。** 这是三个定制中最有意思的一个。问题的核心我们前面已经说过：所有 Region 共享一个 RocksDB，如何为单个 Region 生成快照？

NeoKV 的解决思路是通过 braft 的 `FileSystemAdaptor` 接口，将快照"伪装"成两个虚拟文件：`region_data_snapshot.sst`（Region 在 DATA_CF 中的数据）和 `region_meta_snapshot.sst`（Region 在 METAINFO_CF 中的元数据）。这两个文件并不真实存在于磁盘上。当 braft 尝试读取它们时，NeoKV 的自定义 Adaptor 会从 RocksDB 中实时流式读取对应 Region 的数据。对 braft 来说，它以为自己在读文件；实际上它在读 RocksDB 的一个一致性快照。这种"骗过框架"的做法在系统编程中很常见——接口不变，实现偷天换日。

在 Leader 端，`RocksdbReaderAdaptor` 创建一个 RocksDB Snapshot，然后基于这个 Snapshot 流式读取该 Region key 范围内的所有 KV 对。使用 RocksDB Snapshot 是关键——它保证了即使快照传输过程中有新的写入，读到的仍然是创建 Snapshot 时刻的一致视图。这就像给数据库拍了一张照片，之后不管数据怎么变，这张照片不会变。

在 Follower 端，`SstWriterAdaptor` 将收到的数据写入 SST 文件，然后通过 `IngestExternalFile` 导入 RocksDB。`IngestExternalFile` 直接将预排序的 SST 文件放入 LSM-Tree 的合适层级，跳过了 MemTable 和 WAL，避免了逐条写入的写放大。整个快照流程串起来看：

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

从 braft 的视角看，这就是一次普通的文件快照传输。但在底层，Leader 端是从共享 RocksDB 中流式读取单个 Region 的数据，Follower 端是通过 SST Ingest 高效地将数据导入共享 RocksDB。整个过程没有生成任何临时的完整数据副本，内存开销可控。

**IndexTermMap：一个小而精的内存索引。** Raft 协议在选举和日志比较时需要频繁查询"某个 log index 对应的 Term 是多少"。如果每次都去 RocksDB 里查，延迟太高。所以 NeoKV 在内存中维护了一个基于 `std::deque` 的轻量级索引（`include/raft/index_term_map.h`）。为什么用 deque 而不是 map？因为 Raft 日志的 index 是连续递增的，不会有空洞。deque 的内存布局比 map 紧凑得多（没有树节点的指针开销），而且支持两端的 O(1) 操作——追加新条目在尾部，截断旧条目在头部，正好对应 Raft 日志的 append 和 truncate_prefix。查询时用二分查找，O(log n) 的复杂度对于这个场景绰绰有余。

## Region：不只是一个数据分片

解决了存储层的问题，接下来看 NeoKV 最核心的概念——Region。

在很多分布式存储系统里，"分片"只是一个逻辑上的数据划分。但在 NeoKV 中，Region 同时承担三个角色：它是**数据分片**，管理一段连续的 Redis Slot 范围（0-16383 中的一个子集）；它是 **Raft 状态机**，继承 `braft::StateMachine`，是 Raft 共识的执行体；它也是**服务单元**，接收并处理 Redis 读写请求。如果你读过上一章的 Counter 示例，可以这样理解：Region 就是一个"加强版的 Counter"——Counter 的状态是一个 `int64_t`，Region 的状态是一段 RocksDB 中的 key-value 数据；Counter 只处理 `fetch_add`，Region 处理所有 Redis 命令。

每个 Region 的 Raft 组以 `"region_" + region_id` 命名。启动时，Region 用我们前面讲的自定义存储后端初始化 `braft::Node`：

```cpp
void Region::start_raft() {
    braft::NodeOptions options;
    options.fsm = this;  // Region 自身就是状态机
    options.log_uri = "myraftlog://my_raft_log?id=" + std::to_string(_region_id);
    options.raft_meta_uri = "myraftmeta://my_raft_meta?id=" + std::to_string(_region_id);
    // ...
    braft::GroupId group_id = "region_" + std::to_string(_region_id);
    _node = new braft::Node(group_id, peer_id);
    _node->init(options);
}
```

这个命名约定让 braft 内部能区分不同的 Raft 组，同时也方便日志排查——当你在几百行日志里搜 `region_42` 的时候，你会感谢这个设计。

## 写入路径

一个 Redis 写命令（比如 `SET key value`）从接收到执行，经历这样的旅程：

```
CommandHandler::Run()
    │
    ▼
write_through_raft()
    │  构建 RedisWriteRequest protobuf
    ▼
Region::exec_redis_write()
    │  检查 Leader 身份
    │  构建 StoreReq (op_type = OP_REDIS_WRITE)
    │  序列化为 braft::Task
    ▼
braft::Node::apply(task)
    │  Raft 日志复制
    │  多数节点确认
    ▼
Region::on_apply()
    │  反序列化 StoreReq
    ▼
Region::apply_redis_write()
    │  执行实际的 RocksDB 写入
    │  WriteBatch 保证原子性
    ▼
DMLClosure::Run()
    │  唤醒等待的调用者
    ▼
返回结果给 Client
```

如果你读过上一章的 Counter 示例，这个流程应该很眼熟——本质上就是 Counter 的 `fetch_add` 流程，只是把"计数器加一"换成了"执行 Redis 命令"。我们来看几个关键环节。

`exec_redis_write()` 是写入路径的入口。它将 Redis 命令打包成 protobuf，通过 Raft 提交。这里值得注意的是 `DMLClosure` 和 `BthreadCond` 的配合——上一章我们讲过，braft 的 `apply()` 是异步的，但 Redis 客户端期望同步响应：发一个 SET，等一个 OK。NeoKV 通过 `BthreadCond`（一个基于 bthread 的条件变量）将异步操作转为同步等待：调用端 `cond.increase()` 后阻塞在 `cond.wait()` 上，当 Raft apply 完成后 `DMLClosure::Run()` 调用 `cond.decrease_signal()` 唤醒等待者。`BthreadCond` 阻塞时不会占用 OS 线程，非常适合高并发场景。这个模式本质上就是 Counter 示例中 `brpc::ClosureGuard` + `done->Run()` 的变体，只是 NeoKV 用了一个更适合批量操作的计数器方案。

当 Raft 日志被 committed 后，braft 调用 `on_apply()`。这和 Counter 示例的结构完全一样——遍历 Iterator，反序列化请求，通过 `do_apply()` 分发到具体的处理函数。`apply_redis_write()` 在 `src/redis/region_redis.cpp` 中实现，包含所有 Redis 写命令的执行逻辑，我们将在 Part 3 中详细讨论每种数据类型的实现。

**Leader 延迟激活。** 这里有一个微妙但重要的细节，也是 Counter 示例中没有体现的一个生产级考量：`on_leader_start()` 并不直接将 Region 标记为 Leader。它会先提交一条特殊的 `OP_CLEAR_APPLYING_TXN` Raft 日志，只有当这条日志被 apply 后，才真正设置 `_is_leader = true`。为什么要这样做？因为新 Leader 可能有一些未 apply 的旧日志——比如 Region 正在执行 split 或者成员变更。如果在这些旧日志 apply 之前就开始接受新请求，可能导致状态不一致。通过提交一条新日志并等待它被 apply，我们确保所有旧日志都已经执行完毕，新 Leader 可以安全地开始服务。Counter 的状态太简单了，不需要这种保护。但在真实的存储系统中，这种防御性设计几乎是必须的。

## 读取路径

读操作不需要经过 Raft 共识（不修改数据），但需要保证读到的数据是最新的。NeoKV 支持两种模式。

**Leader 直接读**是最快的路径。如果当前节点是 Region 的 Leader，直接读本地 RocksDB，只需一次本地查询。但严格来说，在网络分区的极端情况下，旧 Leader 可能不知道自己已经被取代——这和上一章 Counter 示例中讨论的问题一样。braft 通过 Lease 机制缓解这个问题：只要 Leader 在最近的心跳周期内收到了多数节点的响应，就认为自己仍然是合法 Leader。

**Follower ReadIndex** 是另一种选择。当配置 `use_read_index=true` 时，Follower 也可以处理读请求。它的流程是：Follower 向 Leader 发送 ReadIndex 请求，Leader 确认自己仍是 Leader 后返回当前 committed index，Follower 等待本地 apply 追上这个 index，然后读取本地 RocksDB。这样就把读负载分散到了所有节点上。`follower_read_wait()` 还做了一个批量收集优化——多个并发读请求合并为一次 ReadIndex RPC，如果 100 个读请求几乎同时到达，它们共享一次 ReadIndex RPC，而不是各自发起 100 次。

当 `use_read_index=false`（默认）时，Follower 不处理读请求，而是返回 MOVED 重定向，告诉客户端去找 Leader。这与 Redis Cluster 的行为一致，Redis 客户端 SDK 通常内置了 MOVED 重定向的处理逻辑。NeoKV 默认不启用 ReadIndex，是因为对大多数场景来说，Leader 直接读已经足够快，而 ReadIndex 虽然能分散读负载，但每次都多一轮 RPC 往返。

## Store：管理数百个 Region 的艺术

每个 neoStore 进程管理本节点上的所有 Region。`Store` 类（`include/store/store.h`）是这个管理者。

在这一层最有意思的设计是**无锁 Region 查找**。每个 Redis 命令都需要根据 slot 找到对应的 Region，这是最热的路径，每秒发生数十万次，必须极快。NeoKV 使用 brpc 提供的 `DoublyBufferedData` 来实现：

```cpp
typedef butil::DoublyBufferedData<std::unordered_map<int64_t, SmartRegion>> DoubleBufRegion;
DoubleBufRegion _region_mapping;
```

`DoublyBufferedData` 的原理简洁而优雅：它维护两份数据副本。读操作直接读当前活跃的副本，无锁、无原子操作。写操作先修改非活跃副本，然后原子切换活跃指针，最后修改另一份。Region 查找（读）每秒发生数十万次，而 Region 变更（写）只在 split/merge/启停时发生——这种读多写少的场景正是 `DoublyBufferedData` 的甜区。

一个 Region 从创建到销毁，经历这样的生命周期：`init_region()` 从 MetaServer 获取 Region 信息并初始化 RocksDB key 范围，然后 `start_raft()` 创建 `braft::Node` 加入 Raft 组。之后 Region 进入正常服务状态，接受读写命令、参与 Raft 共识。在运行过程中，Region 可能经历 Split（分裂为两个 Region）或 Merge（与相邻 Region 合并）。当 Region 需要下线时，`shutdown_raft()` 退出 Raft 组并清理资源。

Store 还运行多个后台线程来维护系统健康。心跳线程每秒向 MetaServer 上报 Region 状态并接收调度指令。Split 检查线程每 10 秒检查 Region 数据量是否超过阈值。还有 Compaction、快照、Flush 等线程按需触发。这些线程的频率都经过仔细调优——太频繁浪费 CPU，太稀疏又会延迟问题发现。

## Region Split：最复杂的操作

当一个 Region 的数据量超过阈值时，需要分裂为两个 Region。这是 Multi-Raft 架构中最复杂的操作之一，因为它涉及数据的重新划分、Raft 组的创建、路由表的更新，而且整个过程必须保证不丢数据、不中断服务。

```
原始 Region (slot 0-16383)
         │
         │ 数据量超过阈值
         ▼
选择分裂点 (比如 slot 8192)
         │
         ▼
┌────────────────┐  ┌────────────────┐
│   Region 1     │  │   Region 2     │
│  slot 0-8191   │  │ slot 8192-16383│
│  (原 Region)   │  │  (新 Region)   │
└────────────────┘  └────────────────┘
```

Split 的关键步骤是这样的：MetaServer 通过心跳响应告诉 Store 分裂某个 Region，并分配新的 Region ID。Leader 将 Split 操作作为一条 Raft 日志提交，保证所有副本一致执行。在 apply Split 日志时，创建新的 Region 对象。新 Region 需要回放原 Region 的部分日志来获取属于自己的数据。`RedisRouter::rebuild_slot_table()` 更新 slot 到 Region 的映射。最后上报新 Region 的信息给 MetaServer。

Split 过程中有一个关键设计值得单独说：新 Region（version=0）使用 `ExecutionQueue` 异步 apply 日志，避免阻塞 braft 的 apply 线程。当 `OP_ADD_VERSION_FOR_SPLIT_REGION` 日志被 apply 后，新 Region 切换为正常的同步 apply 模式。还记得前面惰性删除中的 `SplitIndexGetter` 吗？它就是在这里发挥作用的——Split 过程中需要回放的日志不能被 compaction filter 误删。

这也是为什么我们在前面设计 `RaftLogCompactionFilter` 时要同时检查 `first_index` 和 `split_index`：

```cpp
return index < std::min(first_index, split_index);
```

只有同时满足"已经被 truncate"和"不再被 split 使用"两个条件的日志，才能安全地被删除。

## 检验你的理解

- 为什么把所有 Raft 组的日志放在同一个 RocksDB CF 中比每组独立文件更好？在什么场景下这个方案可能反而更差？
- 惰性日志删除（通过 Compaction Filter）相比立即删除有什么优缺点？如果 compaction 长时间不触发，会有什么影响？
- 快照传输时，Leader 使用 RocksDB Snapshot 保证一致性。如果快照传输耗时很长，这个 Snapshot 会不会影响 RocksDB 的 compaction？
- 为什么 `on_leader_start()` 不直接设置 `_is_leader=true`，而要先提交一条 Raft 日志？如果直接设置会有什么风险？
- ReadIndex 相比 Lease Read 有什么优缺点？NeoKV 为什么默认不启用 ReadIndex？
- Region Split 过程中，客户端发来的请求如何处理？会不会丢数据或返回错误？

---

> 下一章：[04-MetaServer 与集群管理](./04-MetaServer与集群管理.md) — 我们将了解 MetaServer 如何管理集群元数据、调度 Region、以及自身如何通过 Raft 保证高可用。
