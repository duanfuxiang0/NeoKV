# MetaServer 与集群管理

## 本章概览

在这一章中，我们将了解：

- MetaServer 的架构：它自身如何通过 Raft 实现高可用
- 集群管理：Instance（Store 节点）和内部表的管理
- Region 调度：创建、Split、Merge、Peer 变更、Leader 迁移
- 心跳机制：Store 与 MetaServer 之间的双向通信
- TSO（Timestamp Oracle）：分布式时间戳服务
- 依赖反转模式：raft_store 与 raft_meta 的回调分离

## 1. MetaServer 架构

MetaServer 是 NeoKV 集群的"大脑"，负责管理所有元数据。但它自身也需要高可用——如果 MetaServer 挂了，整个集群就无法进行 Region 调度了。

解决方案很自然：**MetaServer 自身也是一个 Raft 组**。

```
┌─────────────────────────────────────────────┐
│              MetaServer 集群                 │
│                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐    │
│  │  Meta 1  │ │  Meta 2  │ │  Meta 3  │    │
│  │ (Leader) │ │(Follower)│ │(Follower)│    │
│  └────┬─────┘ └──────────┘ └──────────┘    │
│       │                                      │
│       │  Raft 组: "meta_raft" (region_id=0)  │
│       │                                      │
│  ┌────▼──────────────────────────────────┐  │
│  │           MetaRocksDB                  │  │
│  │  (独立的 RocksDB 实例，存储元数据)      │  │
│  └────────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

**关键文件**：
- `include/meta_server/meta_server.h` — MetaServer 入口
- `include/meta_server/meta_state_machine.h` — MetaStateMachine
- `include/meta_server/common_state_machine.h` — CommonStateMachine 基类
- `src/meta_server/meta_state_machine.cpp` — on_apply 分发、快照、心跳处理

### 1.1 CommonStateMachine 基类

MetaServer 的 Raft 状态机有两层继承：

```
braft::StateMachine
    └── CommonStateMachine      ← 通用 Raft 生命周期管理
            └── MetaStateMachine  ← MetaServer 特有的业务逻辑
```

`CommonStateMachine` 提供了通用的 Raft 节点管理：

```cpp
// include/meta_server/common_state_machine.h
class CommonStateMachine : public braft::StateMachine {
    braft::Node _node;
    bool _is_leader = false;

    // 初始化 Raft 节点
    int init(const std::vector<braft::PeerId>& peers);

    // 将请求提交到 Raft
    void process(google::protobuf::RpcController* controller,
                 const pb::MetaManagerRequest* request,
                 pb::MetaManagerResponse* response,
                 google::protobuf::Closure* done);

    // Leader 状态变更
    void on_leader_start(int64_t term) override;
    void on_leader_stop(const braft::LeaderChangeContext& ctx) override;
};
```

`process()` 方法是 MetaServer 处理所有元数据变更请求的入口。它将请求序列化后提交到 Raft，保证所有 MetaServer 节点以相同顺序执行相同的操作。

### 1.2 MetaStateMachine 的 on_apply

当 Raft 日志被 committed 后，`MetaStateMachine::on_apply()` 根据请求类型分发到不同的管理器：

```cpp
// src/meta_server/meta_state_machine.cpp — 简化逻辑
void MetaStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        pb::MetaManagerRequest request;
        request.ParseFromArray(iter.data());

        switch (request.op_type()) {
            // 集群管理
            case pb::OP_ADD_INSTANCE:
            case pb::OP_DROP_INSTANCE:
            case pb::OP_UPDATE_INSTANCE:
                ClusterManager::get_instance()->process(request, response);
                break;

            // Region 管理
            case pb::OP_ADD_REGION:
            case pb::OP_UPDATE_REGION:
            case pb::OP_DROP_REGION:
            case pb::OP_SPLIT_REGION:
            case pb::OP_MERGE_REGION:
                RegionManager::get_instance()->process(request, response);
                break;

            // Schema 管理
            case pb::OP_CREATE_TABLE:
            case pb::OP_DROP_TABLE:
                SchemaManager::get_instance()->process(request, response);
                break;
        }
    }
}
```

### 1.3 MetaServer 快照

MetaServer 的快照比 Store 简单得多——它只需要保存 `MetaRocksDB` 中的所有数据：

```cpp
void MetaStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                                         braft::Closure* done) {
    // 将 MetaRocksDB 的全部数据导出为一个 SST 文件
    auto* sst_writer = new SstFileWriter(options);
    sst_writer->open(snapshot_path);

    auto* iter = MetaRocksDB::get_instance()->new_iterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        sst_writer->Put(iter->key(), iter->value());
    }
    sst_writer->Finish();
    writer->add_file("meta_snapshot.sst");
}
```

## 2. 集群管理

### 2.1 ClusterManager

`ClusterManager` 管理集群的物理拓扑：

```cpp
// include/meta_server/cluster_manager.h
class ClusterManager {
    // Instance（Store 节点）管理
    std::map<std::string, pb::InstanceInfo> _instance_info;

    void add_instance(const pb::MetaManagerRequest& request);
    void drop_instance(const pb::MetaManagerRequest& request);
    void update_instance(const pb::MetaManagerRequest& request);

    // 获取可用的 Store 节点（用于 Region 副本分配）
    void select_instance_for_region(int replica_count,
                                     std::vector<std::string>* instances);
};
```

当需要创建新 Region 或添加副本时，`ClusterManager` 负责选择合适的 Store 节点，考虑因素包括：
- 节点的磁盘使用率
- 节点上已有的 Region 数量（负载均衡）
- 机架/可用区分布（故障域隔离）

### 2.2 SchemaManager 与内部表

NeoKV 虽然不支持 SQL，但继承了 BaikalDB 的 Schema 管理框架。为了让 `SplitCompactionFilter` 和 `SchemaFactory` 正常工作，MetaServer 中创建了一个内部表：

```
Database: __redis__
Table:    kv
```

这个表提供了 `table_id` 和 `index_id`，它们被编码到每个 Redis key 的前缀中（16 字节前缀的后 8 字节）。虽然 NeoKV 只有一张"表"，但这个抽象层让 Compaction Filter 和 Region 初始化流程可以复用 BaikalDB 的成熟代码。

## 3. Region 调度

`RegionManager` 是 MetaServer 中最复杂的组件，负责 Region 的全生命周期管理。

**关键文件**：
- `include/meta_server/region_manager.h` — RegionManager 类定义
- `src/meta_server/region_manager.cpp` — 调度逻辑实现

### 3.1 Region 创建

集群初始化时，MetaServer 创建覆盖全部 16384 个 slot 的初始 Region：

```
初始状态：
┌──────────────────────────────────────┐
│           Region 1                    │
│        Slot 0 - 16383                │
│   三副本分布在 Store 1/2/3           │
└──────────────────────────────────────┘
```

随着数据增长，Region 会自动分裂。

### 3.2 Region Split 调度

Split 的触发和执行是一个多步骤的协作过程：

```
Store                           MetaServer
  │                                │
  │  1. Split 检查线程发现          │
  │     Region 数据量超过阈值       │
  │                                │
  │ ── 心跳上报 Region 状态 ──────► │
  │                                │  2. RegionManager 判断需要 Split
  │                                │     分配新 Region ID
  │                                │     选择分裂点
  │ ◄── 心跳响应: split 指令 ────── │
  │                                │
  │  3. Region Leader 提交          │
  │     Split Raft 日志             │
  │                                │
  │  4. on_apply: 创建新 Region     │
  │     回放日志、更新路由表         │
  │                                │
  │ ── 心跳上报新 Region 信息 ────► │
  │                                │  5. 注册新 Region
  │                                │     更新全局路由
```

### 3.3 Peer 管理

MetaServer 通过心跳下发 Peer 变更指令，维护每个 Region 的副本数：

| 操作 | 场景 | 效果 |
|------|------|------|
| `add_peer` | 副本数不足（节点故障后） | 在新节点上创建 Region 副本 |
| `remove_peer` | 副本数过多（节点恢复后） | 移除多余的副本 |
| `transfer_leader` | 负载不均衡 | 将 Leader 迁移到负载较低的节点 |

这些操作都通过 braft 提供的成员变更接口实现，保证变更过程中的安全性。

### 3.4 负载均衡

MetaServer 持续监控各 Store 节点的负载，通过以下策略实现均衡：

- **Region 数量均衡**：尽量让每个 Store 上的 Region 数量相近
- **Leader 均衡**：避免某个 Store 上集中过多 Leader（Leader 承担写入和部分读取）
- **磁盘均衡**：考虑各节点的磁盘使用率

## 4. 心跳机制

心跳是 Store 与 MetaServer 之间的核心通信机制。

### 4.1 Store 心跳上报

```cpp
// Store 定期发送心跳（每秒一次）
void Store::heartbeat_to_meta() {
    pb::StoreHeartBeatRequest request;

    // 上报节点信息
    request.set_instance_address(local_address);
    request.set_disk_capacity(total_disk);
    request.set_disk_used(used_disk);

    // 上报每个 Region 的状态
    for (auto& [id, region] : _region_mapping) {
        auto* region_info = request.add_region_infos();
        region_info->set_region_id(id);
        region_info->set_is_leader(region->is_leader());
        region_info->set_num_table_lines(region->num_table_lines());
        region_info->set_used_size(region->used_size());
        // ...
    }

    MetaServerInteract::get_instance()->send_request("store_heartbeat",
                                                      request, &response);
    // 处理响应中的调度指令
    process_heartbeat_response(response);
}
```

### 4.2 MetaServer 心跳响应

MetaServer 在处理心跳时，会检查每个 Region 的状态，生成调度指令：

```cpp
// src/meta_server/meta_state_machine.cpp
void MetaStateMachine::store_heartbeat(const pb::StoreHeartBeatRequest& request,
                                        pb::StoreHeartBeatResponse* response) {
    // 更新 Instance 状态
    ClusterManager::get_instance()->update_instance_info(request);

    // 检查每个 Region，生成调度指令
    RegionManager::get_instance()->check_regions(request, response);

    // 可能的指令：
    // - response->add_split_regions(region_id, split_key)
    // - response->add_add_peers(region_id, new_peer)
    // - response->add_remove_peers(region_id, old_peer)
    // - response->add_transfer_leaders(region_id, new_leader)
}
```

### 4.3 心跳驱动的设计哲学

NeoKV（继承自 BaikalDB）采用**心跳驱动**而非**主动推送**的调度模式：

- MetaServer **不主动连接** Store，只在心跳响应中下发指令
- Store **定期拉取**调度指令，按自己的节奏执行
- 如果某次心跳丢失，下次心跳会重新获取指令

这种设计的好处：
- **简化网络模型**：MetaServer 不需要维护到每个 Store 的长连接
- **天然幂等**：Store 可以安全地重试心跳
- **故障容忍**：MetaServer 短暂不可用时，Store 继续服务已有 Region

## 5. TSO（Timestamp Oracle）

`TSOStateMachine` 提供分布式时间戳服务：

```cpp
// include/meta_server/tso_state_machine.h
class TSOStateMachine : public braft::StateMachine {
    // 生成全局唯一、单调递增的时间戳
    int64_t gen_tso();
};
```

TSO 的时间戳格式：`[物理时间(ms)][逻辑计数器]`。物理时间保证粗粒度的单调性，逻辑计数器保证同一毫秒内的唯一性。

TSO 在 NeoKV 当前的 Redis 场景中使用较少，但它是分布式事务（如未来可能实现的 MULTI/EXEC）的基础设施。

## 6. 依赖反转模式

NeoKV 的 Raft 层（`src/raft/`）被 Store 和 MetaServer 两个二进制共享。但某些 Raft 回调在两个场景下有不同的行为。NeoKV 通过**依赖反转**解决这个问题：

```
include/raft/
├── split_index_getter.h      ← 接口（纯虚基类 + 单例）
├── can_add_peer_setter.h     ← 接口
└── update_region_status.h    ← 接口

src/raft_store/               ← Store 端实现
├── split_index_getter.cpp    → 委托给 Store 实例查询 split index
├── can_add_peer_setter.cpp   → 委托给 Store 实例判断
└── update_region_status.cpp  → 委托给 Store 实例更新

src/raft_meta/                ← MetaServer 端实现
├── split_index_getter.cpp    → 返回 INT_FAST64_MAX（不需要 split 保护）
├── can_add_peer_setter.cpp   → 空操作
└── update_region_status.cpp  → 空操作
```

编译时，`neoStore` 链接 `src/raft_store/` 的实现，`neoMeta` 链接 `src/raft_meta/` 的实现。这样 Raft 层的代码不需要知道自己运行在哪个进程中，通过接口调用即可获得正确的行为。

以 `SplitIndexGetter` 为例：

```cpp
// include/raft/split_index_getter.h — 接口
class SplitIndexGetter {
public:
    static SplitIndexGetter* get_instance();
    virtual int64_t get(int64_t region_id) = 0;
};

// src/raft_store/split_index_getter.cpp — Store 端
int64_t SplitIndexGetter::get(int64_t region_id) {
    auto region = Store::get_instance()->get_region(region_id);
    if (region) return region->get_split_index();
    return INT_FAST64_MAX;
}

// src/raft_meta/split_index_getter.cpp — MetaServer 端
int64_t SplitIndexGetter::get(int64_t region_id) {
    return INT_FAST64_MAX;  // MetaServer 不需要 split 保护
}
```

`RaftLogCompactionFilter` 调用 `SplitIndexGetter::get_instance()->get(region_id)` 时，会自动获得当前进程对应的实现。

## 7. 代码导读

| 文件 | 内容 |
|------|------|
| `include/meta_server/common_state_machine.h` | CommonStateMachine 基类：Raft 节点管理 |
| `include/meta_server/meta_state_machine.h` | MetaStateMachine：on_apply 分发 |
| `src/meta_server/meta_state_machine.cpp` | 心跳处理、快照保存/加载 |
| `include/meta_server/cluster_manager.h` | ClusterManager：Instance 管理 |
| `include/meta_server/region_manager.h` | RegionManager：Region 调度 |
| `include/meta_server/schema_manager.h` | SchemaManager：内部表管理 |
| `include/meta_server/tso_state_machine.h` | TSOStateMachine：时间戳服务 |
| `include/meta_server/meta_rocksdb.h` | MetaRocksDB：元数据持久化 |
| `src/raft_store/` | Store 端 Raft 回调实现 |
| `src/raft_meta/` | MetaServer 端 Raft 回调实现 |

## 检验你的理解

- MetaServer 自身通过 Raft 保证高可用。如果 MetaServer 的 Leader 宕机，在新 Leader 选出之前，Store 节点会受到什么影响？已有的 Region 还能正常服务吗？
- 心跳驱动的调度模式有什么局限性？如果需要紧急迁移某个 Region（比如节点即将下线），心跳间隔会不会成为瓶颈？
- 为什么 NeoKV 需要一个内部表 `__redis__.kv`？如果去掉这个表，哪些组件会受到影响？
- 依赖反转模式在这里解决了什么问题？如果不用这种模式，还有什么替代方案？
- TSO 生成的时间戳需要全局唯一且单调递增。如果 TSO 的 Leader 发生切换，如何保证新 Leader 生成的时间戳不会与旧 Leader 的重复？

---

> Part 1 到此结束。我们已经完整了解了 NeoKV 的分布式架构：从 Raft 共识算法，到 braft 的工程定制，到 Region 状态机和 Store 管理，再到 MetaServer 的集群调度。
>
> 接下来进入 [Part 2: 存储引擎](../part2-存储引擎/05-RocksDB基础与特性.md)，我们将深入 RocksDB 的世界，理解 NeoKV 的存储层设计。
