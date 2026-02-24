# Multi-Raft 与 Region

## 本章概览

在这一章中，我们将了解：

- Region 作为 Raft 状态机的完整生命周期
- 写入路径：Redis 命令如何通过 Raft 共识到达 RocksDB
- 读取路径：Leader 直接读与 Follower ReadIndex
- Store 如何管理数百个 Region 并保持高性能
- DMLClosure 模式：同步等待异步 Raft 操作的技巧

这一章是 Part 1 的核心，连接了 Raft 共识（第 1-2 章）与上层 Redis 命令处理（Part 3-4）。

## 1. Region：数据分片的基本单位

在 NeoKV 中，Region 是最核心的概念。它同时承担三个角色：

- **数据分片**：管理一段连续的 Redis Slot 范围（0-16383 中的一个子集）
- **Raft 状态机**：继承 `braft::StateMachine`，是 Raft 共识的执行体
- **服务单元**：接收并处理 Redis 读写请求

```
┌─────────────────────────────────────────────┐
│                  Region                      │
│                                              │
│  Slot 范围: [0, 5460]                        │
│  Raft 组:   "region_1"                       │
│  角色:      Leader / Follower / Learner      │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │         braft::StateMachine            │  │
│  │                                        │  │
│  │  on_apply()          → 执行写操作      │  │
│  │  on_leader_start()   → Leader 激活     │  │
│  │  on_leader_stop()    → Leader 卸任     │  │
│  │  on_snapshot_save()  → 保存快照        │  │
│  │  on_snapshot_load()  → 加载快照        │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │         Redis 命令处理                  │  │
│  │                                        │  │
│  │  exec_redis_write()  → 提交写入 Raft   │  │
│  │  apply_redis_write() → 执行 RocksDB 写 │  │
│  │  follower_read_wait()→ ReadIndex 读    │  │
│  └────────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

**关键文件**：
- `include/store/region.h` — Region 类定义
- `src/store/region.cpp` — 状态机回调、Raft 操作
- `src/redis/region_redis.cpp` — Redis 写命令的 apply 逻辑

### 1.1 Raft 组命名

每个 Region 的 Raft 组以 `"region_" + region_id` 命名：

```cpp
// src/store/region.cpp
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

这个命名约定让 braft 内部能区分不同的 Raft 组，同时也方便日志排查。

## 2. 写入路径

一个 Redis 写命令（比如 `SET key value`）从接收到执行，经历以下步骤：

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
Region::do_apply()
    │  根据 op_type 分发
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

### 2.1 提交 Raft 任务

`exec_redis_write()` 是写入路径的入口。它将 Redis 命令打包成 protobuf，通过 Raft 提交：

```cpp
// src/store/region.cpp — 简化逻辑
void Region::exec_redis_write(const pb::StoreReq& request,
                               pb::StoreRes* response,
                               BthreadCond& cond) {
    // 1. 检查是否是 Leader
    if (!is_leader()) {
        response->set_errcode(NOT_LEADER);
        return;
    }

    // 2. 构建 Raft Task
    braft::Task task;
    task.data = &request_buf;  // 序列化的 StoreReq

    // 3. 创建 DMLClosure（异步回调）
    DMLClosure* closure = new DMLClosure(&cond, response, this);
    task.done = closure;

    // 4. 追踪 in-flight 写入
    _real_writing_cond.increase();

    // 5. 提交到 Raft
    _node->apply(task);
}
```

### 2.2 DMLClosure：同步等待异步操作

Raft 的 `apply()` 是异步的——它立即返回，日志复制和应用在后台进行。但 Redis 客户端期望同步响应。NeoKV 通过 `DMLClosure` + `BthreadCond` 实现同步等待：

```cpp
// 调用端（CommandHandler）
BthreadCond cond;
cond.increase();                    // 计数器 +1
region->exec_redis_write(req, &res, cond);
cond.wait();                        // 阻塞等待计数器归零
// 此时 res 中已有结果

// 回调端（Raft apply 完成后）
void DMLClosure::Run() {
    // 设置响应（affected_rows、errmsg 等）
    _cond->decrease_signal();       // 计数器 -1，唤醒等待者
    _real_writing_cond->decrease_signal();
}
```

`BthreadCond` 是基于 bthread（brpc 的协程）的条件变量，阻塞时不会占用 OS 线程，非常适合高并发场景。

### 2.3 日志应用

当 Raft 日志被 committed 后，braft 调用 `on_apply()`：

```cpp
// src/store/region.cpp
void Region::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        // 反序列化 StoreReq
        pb::StoreReq request;
        request.ParseFromArray(iter.data().data(), iter.data().size());

        // 分发到具体的处理函数
        do_apply(request, response);

        // 更新 applied index
        _done_applied_index = iter.index();

        // 触发 Closure 回调（如果是 Leader）
        if (iter.done()) {
            static_cast<DMLClosure*>(iter.done())->Run();
        }
    }
}

void Region::do_apply(const pb::StoreReq& req, pb::StoreRes* res) {
    switch (req.op_type()) {
        case pb::OP_REDIS_WRITE:
            apply_redis_write(req, res);  // → region_redis.cpp
            break;
        // ... 其他操作类型（split、成员变更等）
    }
}
```

`apply_redis_write()` 在 `src/redis/region_redis.cpp` 中实现，包含所有 Redis 写命令的执行逻辑。我们将在 Part 3 中详细讨论每种数据类型的实现。

### 2.4 Leader 延迟激活

一个微妙但重要的细节：`on_leader_start()` 并不直接将 Region 标记为 Leader。

```cpp
// src/store/region.cpp
void Region::on_leader_start(int64_t term) {
    // 不直接设置 _is_leader = true！
    // 而是提交一条特殊的 Raft 日志
    pb::StoreReq req;
    req.set_op_type(pb::OP_CLEAR_APPLYING_TXN);
    braft::Task task;
    task.data = &req_buf;
    _node->apply(task);
}
```

只有当这条 `OP_CLEAR_APPLYING_TXN` 日志被 apply 后，才真正设置 `_is_leader = true`：

```cpp
void Region::leader_start(int64_t term) {
    _is_leader = true;
    // 现在可以开始接受客户端请求了
}
```

为什么要这样做？因为新 Leader 可能有一些未 apply 的旧日志。如果在这些旧日志 apply 之前就开始接受新请求，可能导致状态不一致。通过提交一条新日志并等待它被 apply，我们确保所有旧日志都已经执行完毕。

## 3. 读取路径

读操作不需要经过 Raft 共识（不修改数据），但需要保证读到的数据是最新的。NeoKV 支持两种读取模式：

### 3.1 Leader 直接读

如果当前节点是 Region 的 Leader，直接读本地 RocksDB：

```cpp
// 读命令的通用流程（redis_service.cpp 中的 CommandHandler）
void GetCommandHandler::Run() {
    // 1. 路由到 Region
    auto result = RedisRouter::route(slot);

    // 2. 检查读一致性
    if (region->is_leader()) {
        // Leader 直接读
        read_from_rocksdb(key, &value);
    } else {
        // Follower 处理（见 3.2）
    }
}
```

Leader 直接读是最快的路径，只需一次本地 RocksDB 查询。但严格来说，这依赖于一个假设：**当前节点确实还是 Leader**。在网络分区的极端情况下，旧 Leader 可能不知道自己已经被取代。braft 通过 Lease 机制缓解这个问题。

### 3.2 Follower ReadIndex

当 `use_read_index=true` 时，Follower 也可以处理读请求，通过 ReadIndex 协议保证线性一致性：

```
Follower                          Leader
   │                                │
   │  1. 收到读请求                  │
   │                                │
   │ ── ask_leader_read_index() ──► │
   │                                │  2. Leader 确认自己仍是 Leader
   │                                │     返回当前 committed index
   │ ◄── read_index = 12345 ─────── │
   │                                │
   │  3. 等待本地 apply 追上
   │     wait(_done_applied_index >= 12345)
   │
   │  4. 读取本地 RocksDB
   │     read_from_rocksdb(key, &value)
```

`follower_read_wait()` 的实现使用了批量收集优化：

```cpp
// src/store/region.cpp — 简化逻辑
void Region::follower_read_wait(int64_t* read_index) {
    // 批量收集：多个并发读请求合并为一次 ReadIndex RPC
    _read_index_queue.execute([&]() {
        // 向 Leader 获取 committed index
        *read_index = ask_leader_read_index();
    });

    // 等待本地 apply 追上
    while (_done_applied_index < *read_index) {
        bthread_usleep(100);  // 短暂等待
    }

    // 现在可以安全地读取本地数据了
}
```

批量收集的好处：如果 100 个读请求几乎同时到达，它们共享一次 ReadIndex RPC，而不是各自发起 100 次。

### 3.3 MOVED 重定向

当 `use_read_index=false`（默认）时，Follower 不处理读请求，而是返回 MOVED 重定向，告诉客户端去找 Leader：

```
Client                    Follower                  Leader
  │                          │                        │
  │ ── GET key ────────────► │                        │
  │                          │                        │
  │ ◄─ -MOVED 1234 10.0.0.1:16379                    │
  │                                                   │
  │ ── GET key ──────────────────────────────────────► │
  │ ◄─ "value" ──────────────────────────────────────  │
```

这与 Redis Cluster 的行为一致，Redis 客户端 SDK 通常内置了 MOVED 重定向的处理逻辑。

## 4. Store：管理多个 Region

每个 neoStore 进程管理本节点上的所有 Region。`Store` 类是这个管理者。

**关键文件**：
- `include/store/store.h` — Store 类定义
- `src/store/store.cpp` — 初始化、Region 生命周期、后台线程

### 4.1 无锁 Region 查找

每个 Redis 命令都需要根据 slot 找到对应的 Region。这是最热的路径，必须极快。NeoKV 使用 brpc 提供的 `DoublyBufferedData` 实现无锁读：

```cpp
// include/store/store.h
typedef butil::DoublyBufferedData<std::unordered_map<int64_t, SmartRegion>> DoubleBufRegion;
DoubleBufRegion _region_mapping;
```

`DoublyBufferedData` 的原理：
- 维护两份数据的副本（A 和 B）
- **读操作**：直接读当前活跃的副本，无锁、无原子操作
- **写操作**：先修改非活跃副本，然后原子切换活跃指针，最后修改另一份

这种设计在读多写少的场景下非常高效。Region 查找（读）每秒发生数十万次，而 Region 变更（写）只在 split/merge/启停时发生。

### 4.2 Region 生命周期

```
创建 Region
    │
    ▼
init_region()
    │  从 MetaServer 获取 Region 信息
    │  初始化 RocksDB key 范围
    ▼
start_raft()
    │  创建 braft::Node
    │  加入 Raft 组
    ▼
服务请求
    │  接受读写命令
    │  参与 Raft 共识
    ▼
Split / Merge (可选)
    │  Region 分裂或合并
    ▼
shutdown_raft()
    │  退出 Raft 组
    │  清理资源
    ▼
销毁 Region
```

### 4.3 后台线程

Store 运行多个后台线程来维护系统健康：

| 线程 | 职责 | 频率 |
|------|------|------|
| 心跳线程 | 向 MetaServer 上报 Region 状态，接收调度指令 | 每秒 |
| Split 检查 | 检查 Region 数据量是否超过阈值，触发分裂 | 每 10 秒 |
| Compaction | 触发 RocksDB compaction | 按需 |
| 快照 | 定期触发 Raft 快照 | 按配置 |
| Flush | 触发 RocksDB memtable flush | 按需 |

### 4.4 心跳与调度

Store 通过心跳与 MetaServer 保持通信。心跳是双向的：

```
Store → MetaServer (上报):
  - 每个 Region 的 Leader/Follower 状态
  - Region 的数据量、key 数量
  - 节点的磁盘使用率、负载

MetaServer → Store (下发):
  - split_region: 分裂某个 Region
  - add_peer / remove_peer: 增减 Raft 副本
  - transfer_leader: 迁移 Leader
```

这种心跳驱动的调度模式让 MetaServer 不需要主动连接 Store，降低了系统复杂度。

## 5. Region Split

当一个 Region 的数据量超过阈值时，需要分裂为两个 Region。这是 Multi-Raft 架构中最复杂的操作之一。

### 5.1 Split 流程概览

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

### 5.2 Split 的关键步骤

1. **MetaServer 下发 Split 指令**：通过心跳响应告诉 Store 分裂某个 Region
2. **Leader 提交 Split 日志**：将 Split 操作作为一条 Raft 日志提交，保证所有副本一致执行
3. **创建新 Region**：在 apply Split 日志时，创建新的 Region 对象
4. **日志回放**：新 Region 需要回放原 Region 的部分日志，获取属于自己的数据
5. **更新路由表**：`RedisRouter::rebuild_slot_table()` 更新 slot 到 Region 的映射
6. **通知 MetaServer**：上报新 Region 的信息

Split 过程中的一个关键设计：新 Region（version=0）使用 `ExecutionQueue` 异步 apply 日志，避免阻塞 braft 的 apply 线程。当 `OP_ADD_VERSION_FOR_SPLIT_REGION` 日志被 apply 后，新 Region 切换为正常的同步 apply 模式。

## 6. 代码导读

| 文件 | 内容 |
|------|------|
| `include/store/region.h` | Region 类定义：StateMachine 接口、读写方法、Split 参数 |
| `src/store/region.cpp` | on_apply、on_leader_start/stop、exec_redis_write、follower_read_wait |
| `src/redis/region_redis.cpp` | apply_redis_write：所有 Redis 写命令的 Raft apply 逻辑 |
| `include/store/closure.h` | DMLClosure、SplitClosure、AddPeerClosure 等回调定义 |
| `src/store/closure.cpp` | Closure::Run() 实现 |
| `include/store/store.h` | Store 类：DoubleBufRegion、后台线程 |
| `src/store/store.cpp` | Store 初始化、Region 生命周期管理 |

## 检验你的理解

- 为什么 `on_leader_start()` 不直接设置 `_is_leader=true`，而要先提交一条 Raft 日志？如果直接设置会有什么风险？
- `DMLClosure` + `BthreadCond` 的模式本质上是将异步操作转为同步。这种模式在什么情况下可能成为性能瓶颈？
- ReadIndex 相比 Lease Read 有什么优缺点？NeoKV 为什么默认不启用 ReadIndex（`use_read_index=false`）？
- `DoublyBufferedData` 适合读多写少的场景。如果 Region 频繁 Split/Merge，这个数据结构还合适吗？
- Region Split 过程中，客户端发来的请求如何处理？会不会丢数据或返回错误？

---

> 下一章：[04-MetaServer 与集群管理](./04-MetaServer与集群管理.md) — 我们将了解 MetaServer 如何管理集群元数据、调度 Region、以及自身如何通过 Raft 保证高可用。
