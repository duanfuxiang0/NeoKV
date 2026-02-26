# MetaServer 与集群管理

## 概览

- MetaServer 的架构：它自身如何通过 Raft 实现高可用
- 集群管理：Store 节点的注册、发现与负载均衡
- Region 调度：创建、Split、Merge、副本变更、Leader 迁移
- 心跳驱动的调度哲学
- 依赖反转：raft_store 与 raft_meta 如何共享一套 Raft 代码

## MetaServer：集群的大脑

前三章我们一直在 Store 节点的视角里打转——Raft 共识、braft 实践、Region 状态机。但一个分布式存储系统不能只有数据节点。谁来决定"这个 Region 应该分裂"？谁来决定"新副本放在哪台机器上"？谁来维护全局的路由表，告诉客户端"slot 8192 在哪个 Region 上"？

这些问题都指向同一个答案：MetaServer。

MetaServer 是 NeoKV 集群的调度中心，负责管理所有元数据——哪些 Store 节点在线、每个 Region 的副本分布在哪里、slot 到 Region 的映射关系、Region 的健康状态。如果把 Store 比作一线的工人，MetaServer 就是车间主任：它不直接搬砖，但所有关于"谁去做什么"的决策都由它做出。

但这里有一个经典的鸡生蛋的问题：如果 MetaServer 是单点的，它自己挂了怎么办？整个集群就无法进行调度了。解决方案很自然——**MetaServer 自身也是一个 Raft 组**。

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

三个 MetaServer 节点组成一个 Raft 组，所有元数据的变更（添加节点、Region 分裂、副本迁移）都通过 Raft 日志复制到多数节点后才生效。MetaServer 有自己独立的 RocksDB 实例（`MetaRocksDB`），与 Store 节点的数据 RocksDB 完全隔离。这意味着即使 MetaServer 的 Leader 宕机了，新 Leader 选出后可以立刻从本地的 RocksDB 中恢复所有元数据，继续调度。

MetaServer 的 Raft 状态机有两层继承结构。`CommonStateMachine` 是基类，封装了通用的 Raft 节点管理——初始化 `braft::Node`、提交请求到 Raft、处理 Leader 状态变更。它提供了一个 `process()` 方法作为所有元数据变更请求的入口，将请求序列化后提交到 Raft，保证所有 MetaServer 节点以相同顺序执行相同的操作。`MetaStateMachine` 继承 `CommonStateMachine`，实现了 MetaServer 特有的业务逻辑——`on_apply()` 根据请求类型分发到不同的管理器。

这种分层设计的好处是：如果未来需要另一种 Raft 驱动的元数据服务（比如独立的配置中心），可以复用 `CommonStateMachine` 的通用逻辑，只需要实现新的业务状态机。

## 三个管理器

MetaServer 的业务逻辑分散在三个管理器中，每一个负责一个领域。

**ClusterManager** 管理集群的物理拓扑。它维护着一张 Store 节点的注册表——每台 Store 的地址、磁盘容量、已用空间、上面跑了多少个 Region。当需要创建新 Region 或添加副本时，`ClusterManager` 负责选择合适的 Store 节点。选择策略会综合考虑节点的磁盘使用率（别把数据全堆到一台机器上）、已有的 Region 数量（负载均衡）、以及故障域分布（同一个 Region 的三副本最好在不同机架上）。这些策略听起来简单，但在生产环境中你会发现它们之间经常有冲突——比如磁盘最空闲的节点可能和已有副本在同一个机架上。怎么权衡，本质上是一个多目标优化问题。

**SchemaManager** 管理内部表。NeoKV 虽然不支持 SQL，但它继承了 BaikalDB 的 Schema 管理框架。这一层为什么还保留着？因为 NeoKV 的底层有一些组件——比如 `SplitCompactionFilter` 和 `SchemaFactory`——依赖 `table_id` 和 `index_id` 来工作。这些 ID 被编码到每个 Redis key 的前缀中。为了让这些组件正常运转，MetaServer 中创建了一个内部表 `__redis__.kv`。虽然 NeoKV 只有这一张"表"，但这个抽象层让 Compaction Filter 和 Region 初始化流程可以复用 BaikalDB 的成熟代码，不需要重新发明轮子。从工程角度看，这是一个务实的选择——改造已有的代码路径让它"退化"为单表模式，比从零重写一套简单得多。

**RegionManager** 是三个管理器中最复杂的，负责 Region 的全生命周期管理。集群初始化时，它创建覆盖全部 16384 个 slot 的初始 Region。随着数据增长，它决定哪个 Region 需要分裂、分裂点在哪里、新 Region 的 ID 是什么。它还负责副本管理——当某台 Store 宕机导致 Region 副本数不足时，选择一台健康的 Store 添加新副本；当宕机节点恢复导致副本数过多时，移除多余的副本。Leader 均衡也是它的职责——如果某台 Store 上集中了过多的 Leader（Leader 承担写入和部分读取，负载更高），RegionManager 会通过 `transfer_leader` 指令将一些 Leader 迁移到负载较低的节点。

这三个管理器的操作最终都归结为一个模式：构造一个 `MetaManagerRequest` protobuf，通过 `CommonStateMachine::process()` 提交到 Raft。`MetaStateMachine::on_apply()` 在日志 committed 后根据 `op_type` 将请求分发到对应的管理器执行。这保证了所有 MetaServer 节点以完全相同的顺序执行完全相同的操作，元数据在任何时刻都是一致的。

## 心跳驱动的调度

Store 与 MetaServer 之间的核心通信机制是心跳，而不是你可能预期的那种"MetaServer 主动下发指令"的模式。

每秒一次，Store 向 MetaServer 发送一个心跳请求，上报自身的状态——节点地址、磁盘容量和使用率、以及本节点上每个 Region 的详细信息（是否是 Leader、数据量、key 数量）。MetaServer 收到心跳后做两件事：第一，更新自己内部维护的集群状态快照；第二，检查每个 Region 的状态，生成调度指令放在心跳响应中返回。

```
Store                           MetaServer
  │                                │
  │ ── 心跳: 上报 Region 状态 ───► │
  │                                │  检查副本数、数据量、Leader 分布
  │                                │  生成调度指令
  │ ◄── 心跳响应: 调度指令 ──────── │
  │                                │
  │  按指令执行:                    │
  │  - split_region                │
  │  - add_peer / remove_peer      │
  │  - transfer_leader             │
```

这种心跳驱动（而非主动推送）的设计有几个好处。MetaServer 不需要维护到每个 Store 的长连接，简化了网络模型。Store 可以安全地重试心跳，天然幂等。如果某次心跳丢失，下次心跳会重新获取指令，不会遗漏。最重要的是，MetaServer 短暂不可用时（比如 Leader 切换期间），Store 继续服务已有 Region，只是暂时无法执行调度操作。这符合我们"CP 系统"的定位——数据安全和读写服务优先，调度延迟可以容忍。

当然，心跳驱动也有局限。心跳间隔（默认 1 秒）决定了调度响应的最小延迟。如果某台 Store 突然宕机，MetaServer 最快也要在下一个心跳周期才能感知到。不过在实践中，这个延迟通常是可以接受的——Region 有三副本，一台 Store 宕机后 Leader 会自动切换到其他副本上（这是 Raft 自己处理的，不需要 MetaServer 参与），服务不会中断。MetaServer 的调度只是在事后"补齐"副本数。

## Region Split 的完整协作

在上一章我们从 Store 的视角看了 Split 的执行过程。现在让我们从全局视角看 Split 是如何由 MetaServer 和 Store 协作完成的。

整个流程是这样的：Store 的 Split 检查线程（每 10 秒运行一次）发现某个 Region 的数据量超过阈值。下一次心跳上报时，MetaServer 的 RegionManager 收到这个信息，判断确实需要 Split。它为新 Region 分配一个全局唯一的 ID，选择分裂点，然后在心跳响应中下发 split 指令。Store 收到指令后，Region 的 Leader 将 Split 操作作为一条 Raft 日志提交，保证所有副本一致执行。apply Split 日志时创建新 Region，回放日志，更新路由表。最后，Store 通过下一次心跳将新 Region 的信息上报给 MetaServer，MetaServer 注册新 Region 并更新全局路由。

注意这个过程中 MetaServer 从来不直接操作 Store 的数据——它只负责"决策"（需不需要 Split、新 Region ID 是什么），"执行"完全由 Store 自己通过 Raft 共识完成。这种决策与执行分离的设计让系统更容易推理和调试：如果 Split 出了问题，你可以明确地区分是"决策错了"还是"执行错了"。

副本管理（add_peer / remove_peer）和 Leader 迁移（transfer_leader）也遵循同样的协作模式：MetaServer 通过心跳响应下发指令，Store 通过 braft 的成员变更接口执行。这些操作都经过 Raft 共识，保证了变更过程中的安全性。

## TSO：分布式时间戳

NeoKV 的 MetaServer 集群中还运行着一个 `TSOStateMachine`（Timestamp Oracle），提供分布式时间戳服务。它生成的时间戳格式是 `[物理时间(ms)][逻辑计数器]`——物理时间保证粗粒度的单调性，逻辑计数器保证同一毫秒内的唯一性。

TSO 在 NeoKV 当前的 Redis 场景中使用较少，但它是分布式事务（如未来可能实现的 MULTI/EXEC）的基础设施。如果你熟悉 TiDB/TiKV 的架构，会发现这和 PD（Placement Driver）中的 TSO 是同一个设计思路。保留这个能力，是为了给 NeoKV 未来的事务支持留下扩展空间。

## 依赖反转：一套代码，两种行为

最后聊一个工程设计上的巧妙之处。

NeoKV 的 Raft 层代码（`src/raft/`）被两个二进制共享：`neoStore`（数据节点）和 `neoMeta`（元数据节点）。大部分 Raft 逻辑在两个场景下是完全一样的——日志追加、日志截断、元数据持久化。但有少数几个地方的行为不同。比如前面提到的 `RaftLogCompactionFilter`，它在 Store 端需要检查 `SplitIndexGetter` 来保护正在 Split 的日志不被删除，但在 MetaServer 端根本不存在 Split 的概念。

如果用 `#ifdef` 来区分两种场景，代码会变得难以维护。NeoKV 用了一个更优雅的方案——**依赖反转**。在 `include/raft/` 中定义了几个纯虚基类接口（`SplitIndexGetter`、`CanAddPeerSetter`、`UpdateRegionStatus`），然后在 `src/raft_store/` 和 `src/raft_meta/` 中分别提供 Store 端和 MetaServer 端的实现。

以 `SplitIndexGetter` 为例：Store 端的实现会委托给 Store 实例查询对应 Region 的 split index，而 MetaServer 端的实现直接返回 `INT_FAST64_MAX`（意思是"所有日志都可以删"，因为 MetaServer 不做 Split）。编译时，`neoStore` 链接 `src/raft_store/` 的实现，`neoMeta` 链接 `src/raft_meta/` 的实现。Raft 层的代码通过 `SplitIndexGetter::get_instance()->get(region_id)` 调用接口，完全不知道自己运行在哪个进程中，但总能获得正确的行为。

这种设计的价值在于：Raft 层的核心逻辑只有一份代码，减少了重复和 bug 潜伏面。新增一个差异行为只需要添加一个接口和两个实现，不影响已有代码。而且编译期就确定了链接关系，没有运行时开销。如果你在大型 C++ 项目中见过那种到处 `#ifdef STORE` / `#ifdef META` 的代码，就会理解为什么依赖反转是更好的选择。

## 检验你的理解

- MetaServer 自身通过 Raft 保证高可用。如果 MetaServer 的 Leader 宕机，在新 Leader 选出之前，Store 节点会受到什么影响？已有的 Region 还能正常服务吗？
- 心跳驱动的调度模式有什么局限性？如果需要紧急迁移某个 Region（比如节点即将下线），心跳间隔会不会成为瓶颈？
- 为什么 NeoKV 需要一个内部表 `__redis__.kv`？如果去掉这个表，哪些组件会受到影响？
- 依赖反转模式在这里解决了什么问题？如果不用这种模式，还有什么替代方案？各有什么优劣？
- TSO 生成的时间戳需要全局唯一且单调递增。如果 TSO 的 Leader 发生切换，如何保证新 Leader 生成的时间戳不会与旧 Leader 的重复？

---

> Part 1 到此结束。我们已经完整了解了 NeoKV 的分布式架构：从 Raft 共识算法，到 braft 的工程定制，到 Region 状态机和 Store 管理，再到 MetaServer 的集群调度。
>
> 接下来进入 [Part 2: 存储引擎](../2-存储引擎/05-RocksDB基础与特性.md)，我们将深入 RocksDB 的世界，理解 NeoKV 的存储层设计。
