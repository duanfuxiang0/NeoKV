# Raft 共识算法

## 概览

- 为什么需要共识算法
- Raft 的核心机制：Leader 选举、日志复制、安全性保证
- 从单 Raft 到 Multi-Raft 组

如果你已经熟悉 Raft，可以直接跳到 [02-braft 实践](./02-braft实践.md)。

## 从一个真实的 Redis 集群说起

在聊 Raft 之前，我想先讲一段亲身经历。这段经历直接影响了 NeoKV 的设计方向，也能帮你理解为什么我们需要一个"更好的"分布式方案。

2019 年前后，我负责维护公司的一套 Redis 缓存集群。那时候业务增长很快，我们需要一个能扛住百万级并发的缓存层。最终我们选择了一套在当时非常主流的方案：Codis 代理 + Redis-Server。整套系统的架构大概长这样：

```
                        ┌─────────────┐
                        │  ZooKeeper  │
                        │  (元数据)    │
                        │  3 nodes    │
                        └──────┬──────┘
                               │ 
            ┌──────────────────┼────────────────┐
            ▼                  ▼                ▼
     ┌────────────┐    ┌────────────┐     ┌────────────┐
     │   Codis    │    │   Codis    │     │   Codis    │
     │  Proxy 1   │    │  Proxy 2   │     │  Proxy 3   │
     └─────┬──────┘    └─────┬──────┘     └─────┬──────┘
           │  Hash.          │                  │
     ┌─────┴───────────┬─────┴────────────┬─────┴─────┐
     │                 |                  │           │
     ▼                 ▼                  ▼           ▼
┌─────────┐      ┌─────────┐      ┌─────────┐   ┌─────────┐
│ Redis   │      │ Redis   │      │ Redis   │   │ Redis   │
│ Master1 │      │ Master2 │      │ Master3 │   │ Master4 │
│ Shard 0 │      │ Shard 1 │      │ Shard 2 │   │ Shard 3 │
└────┬────┘      └────┬────┘      └────┬────┘   └────┬────┘
     │ Sentinel       │ Sentinel       │ Sentinel    │
     ▼                ▼                ▼             ▼
┌─────────┐      ┌─────────┐      ┌─────────┐   ┌─────────┐
│ Redis   │      │ Redis   │      │ Redis   │   │ Redis   │
│ Slave1  │      │ Slave2  │      │ Slave3  │   │ Slave4  │
└─────────┘      └─────────┘      └─────────┘   └─────────┘
```

这套系统的核心思路很简单：Redis-Server 本质上是一个单机内存数据库，它自己不知道"集群"是什么。所以我们需要在它外面包一层：用固定 Hash 分片把数据打散到多台 Redis-Server 上，用 Sentinel 做主从复制和故障切换来保证可用性，用 Codis 作为无状态代理层承接客户端请求并路由到正确的分片，最后用 ZooKeeper 存储集群的元数据（路由表、分片映射、节点状态）并协调整个集群的运维操作。

说实话，这套架构虽然粗糙，但它确实扛住了我们的第一个百万并发。在那个阶段，它是一个务实的选择。

但随着业务跑了一段时间，问题开始浮出水面。

首先是数据一致性。Redis 的 Sentinel 复制是异步的——Master 写入成功后立即返回客户端，然后再异步地把数据同步给 Slave。这意味着如果 Master 在同步完成之前宕机，那些已经告诉客户端"写入成功"的数据就丢了。Sentinel 会把 Slave 提升为新 Master，但 Slave 上缺少最后几秒的数据，而客户端对此一无所知。在缓存场景下这勉强可以接受，但如果你想把它当作主存储，这就是一个定时炸弹。

其次是跨分片操作。因为数据是按固定 Hash 分散到不同 Redis-Server 上的，任何涉及多个 key 的操作（MGET、MSET、事务）都需要 Codis 在代理层做拆分和聚合。跨分片事务？不存在的。你只能在应用层自己想办法，或者干脆避免这类操作。

然后是运维复杂度。你数一下这套系统里有多少种不同类型的进程：Redis-Server、Redis Sentinel、Codis Proxy、Codis Dashboard、ZooKeeper。每一种都有自己的配置、自己的故障模式、自己的监控指标。扩容缩容需要手动迁移分片，过程中还要小心翼翼地避免数据丢失。半夜收到告警，你得先判断是哪个组件出了问题，然后翻对应组件的文档去处理。

最后是持久性。这是 Redis 本身的特性决定的——它是一个内存数据库，RDB 快照和 AOF 日志都不能保证零数据丢失。即使你开了 `appendfsync always`，在主从切换的窗口期仍然可能丢数据。

这些问题并不是我们团队独有的。在 2020 年之前，这种 "代理 + 分片 + 异步复制" 的 Redis 集群架构在国内非常普遍——Codis、Twemproxy、甚至后来 Redis 官方的 Redis Cluster，本质上都是同一个思路的不同变体。它们解决了"单机 Redis 容量和吞吐不够"的问题，但没有解决"数据一致性和持久性"的问题。

## 更好的方案：Raft + RocksDB

如果我们退后一步，重新审视上面那套系统，会发现它的复杂性来源于一个根本矛盾：Redis-Server 是一个不具备分布式能力的单机服务，我们试图用外部组件（Codis、Sentinel、ZooKeeper）给它"贴上"分布式的能力。每多贴一层，系统就多一种故障模式，多一份运维负担。

那如果我们换一个思路呢？如果存储节点自身就具备共识能力——它自己就能决定"这条数据是否已经被多数节点确认"，不需要外部的 Sentinel 来仲裁主从切换，不需要外部的 ZooKeeper 来存储元数据——整个系统的架构会简洁得多。

这就是 Raft 共识算法的价值所在。

Raft 让一组节点（通常是 3 个或 5 个）就"一系列操作的顺序"达成一致。只要多数节点存活，集群就能正常工作。Leader 宕机了？剩下的节点自动选出新 Leader，不需要外部仲裁。数据会不会丢？不会，因为每条数据都经过多数节点确认后才告诉客户端"写入成功"。

把 Raft 和 RocksDB（一个高性能的持久化 KV 引擎）结合起来，我们就得到了一个理想的替代方案：

```
Old:                                   New:

┌──────────────┐                      ┌──────────────────────────┐
│  ZooKeeper   │  Metadata            │        MetaServer        │
│   (3 nodes)  │                      │      (Raft Group)        │
└──────┬───────┘                      └─────────────┬────────────┘
       │                                            │
┌──────┴───────┐                      ┌─────────────┴────────────┐
│ Codis Proxy  │  Proxy + Routing     │       Store Node 1/2/3   │
└──────┬───────┘                      │                          │
       │                              │   ┌──────────────────┐   │
┌──────┴────────┐                     │   │  Region (Raft)   │   │
│     Redis     │  Storage            │   │  RocksDB Engine  │   │
│   Sentinel    │  Replication        │   └──────────────────┘   │
│  Master/Slave │                     │                          │
└───────────────┘                     └──────────────────────────┘
```

左边是我们之前维护的那套系统，5 种不同类型的进程各司其职但也各自为政。右边是 NeoKV 的架构，只有两种进程：MetaServer 负责集群调度（它自身也是一个 Raft 组，不再依赖外部的 ZooKeeper），Store 节点承载数据（每个数据分片是一个独立的 Raft 组，不再需要外部的 Sentinel）。数据复制就是 Raft 日志复制，故障切换就是 Raft Leader 选举，路由信息内建在集群中。

这就是 NeoKV 的设计出发点。接下来我们深入看看 Raft 到底是怎么工作的。

## CAP 定理：我们做了什么取舍

在继续之前，有必要聊一下 CAP 定理。它告诉我们，分布式系统在网络分区（Partition）发生时，只能在一致性（Consistency）和可用性（Availability）之间二选一。

我们之前的 Redis 集群选择了 AP——优先保证可用性。Sentinel 异步复制，主从切换可能丢数据，但服务几乎不中断。这对缓存场景是合理的：丢几条缓存数据，最多回源查一次数据库，不是什么大事。

NeoKV 选择了 CP——优先保证一致性。每次写入都必须经过多数节点确认才返回成功。代价是写入延迟更高（需要至少一次网络往返等待多数节点 ACK），但换来的是：一旦告诉客户端"写入成功"，这条数据就不会丢失，即使 Leader 随后立刻宕机。

这个取舍决定了 NeoKV 的定位：它不是一个缓存系统，而是一个强一致的分布式存储，只是恰好兼容 Redis 协议。

## Paxos 与 Raft：为什么选 Raft

共识算法并不是什么新概念。在 Raft 之前，Paxos 是这个领域的"标准答案"，由 Leslie Lamport 在 1989 年提出。Paxos 在理论上是完备的，但它有一个广为人知的问题：太难理解了。Lamport 的原始论文用希腊议会的比喻来描述算法，连很多分布式系统专家都承认难以完全理解。更麻烦的是，从 Basic Paxos 到可以实际使用的 Multi-Paxos，中间有大量的工程细节论文没有覆盖，每个团队的实现都不太一样。

2014 年，Diego Ongaro 和 John Ousterhout 发表了 [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)，明确提出 Raft 的设计目标就是"可理解性"。Raft 在能力上与 Multi-Paxos 等价，但它做了几个关键的简化：

所有写入都通过一个强 Leader，不存在多个节点同时提议的情况，大幅简化了并发控制。日志条目严格按顺序追加，不允许空洞，这让日志的一致性检查变得非常直观。集群成员变更有明确的协议，不需要停机操作。

这些设计让 Raft 成为工业界最流行的共识算法。TiKV、CockroachDB、etcd、Consul 都基于 Raft。NeoKV 也不例外。

## Raft 核心机制

Raft 将共识问题分解为三个相对独立的子问题：Leader 选举、日志复制、安全性保证。我们逐一来看。

**Leader 选举。** Raft 集群中的每个节点处于三种状态之一：Follower、Candidate、Leader。正常运行时，集群中有一个 Leader 和若干 Follower。Leader 定期发送心跳给 Follower，Follower 收到心跳就重置自己的选举超时计时器。

```
                     Timeout → Start Election
        ┌────────────────────────────────────────┐
        │                                        ▼
    Follower ◄────────────────────────────── Candidate
        ▲            Election Failed             │
        │        or Higher-Term Discovered       │ Majority Votes
        │                                        ▼
        └────────────────────────────────────── Leader
                     Higher Term Discovered
```

如果一个 Follower 在超时时间内没有收到心跳（说明 Leader 可能挂了），它就转为 Candidate，递增自己的 Term（任期号，Raft 的逻辑时钟），然后向其他节点发送 RequestVote RPC 请求投票。每个节点在一个 Term 内只能投一票，先到先得。如果 Candidate 获得多数节点的投票，它就成为新 Leader。如果在选举超时内没有获得多数票（比如多个 Candidate 同时发起选举导致选票分裂），就重新开始一轮选举。

这里有一个关键的安全约束叫 Election Restriction：投票时，节点会检查候选人的日志是否"至少和自己一样新"。这保证了新选出的 Leader 一定包含所有已 committed 的日志，不会出现"选了一个落后的节点当 Leader 导致数据丢失"的情况。

**日志复制。** Leader 选出后，所有写操作都通过 Leader 处理。流程如下：

```
Client          Leader              Follower1          Follower2
  │                │                    │                  │
  │─── SET x=1 ───►│                    │                  │
  │                │── AppendEntries ─► │                  │
  │                │── AppendEntries ───┼─────────────────►│
  │                │                    │                  │
  │                │◄──── ACK ────────  │                  │
  │                │   (Majority)       │                  │
  │                │                    │                  │
  │                │   commitIndex++    │                  │
  │                │   apply to SM      │                  │
  │◄────── OK ─────│                    │                  │
  │                │                    │                  │
  │                │── Heartbeat (with new commit) ──────► │
  │                │                    │   apply to SM    │
```

Client 发送写请求到 Leader。Leader 将操作追加到本地日志，然后并行发送 AppendEntries RPC 给所有 Follower。当多数节点（含 Leader 自己）确认收到日志后，该条目被标记为 committed。Leader 将 committed 的日志应用到状态机（在 NeoKV 中就是写入 RocksDB），返回结果给 Client。Follower 在后续心跳中得知新的 commit index，也将日志应用到自己的状态机。

这就是 Raft 的核心保证：一旦一条日志被 committed，它就不会丢失（只要多数节点存活）。回想一下我们之前 Redis Sentinel 的问题——Master 写入成功就返回客户端，Slave 可能还没收到数据。Raft 彻底解决了这个问题：不是"写入一个节点就算成功"，而是"多数节点确认才算成功"。

**安全性保证。** Raft 通过几个性质共同保证了系统的正确性：

Leader Completeness——如果一条日志在某个 Term 被 committed，那么所有更高 Term 的 Leader 都包含这条日志。这通过选举时的 Election Restriction 保证。State Machine Safety——如果某个节点在某个 index 应用了一条日志，那么其他节点在同一 index 不会应用不同的日志。Log Matching——如果两个节点的日志在某个 index 处的 Term 相同，那么它们在该 index 及之前的所有日志都相同。

这些性质共同保证了：所有节点的状态机最终会以相同的顺序执行相同的操作。换句话说，无论你读哪个节点（在正确的协议下），看到的数据都是一致的。

## 从单 Raft 到 Multi-Raft

理解了单个 Raft 组的工作原理后，一个自然的问题是：一个 Raft 组够用吗？

答案是不够。单个 Raft 组有三个明显的瓶颈：所有写入都经过单一 Leader，写入吞吐无法水平扩展；单个 Raft 组的数据量受限于单机存储容量；所有请求集中在一个 Leader 上，容易形成热点。

回想我们之前的 Redis 集群，它用固定 Hash 分片解决了容量和吞吐的问题（虽然引入了其他问题）。Multi-Raft 的思路类似，但更优雅：将数据分成多个分片（NeoKV 中叫 Region），每个分片是一个独立的 Raft 组，拥有自己的 Leader、自己的日志、自己的选举。

```
┌──────────────────────────────────────────────────────────────┐
│                         Data Space                           │
│                                                              │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐      │
│  │   Region 1   │   │   Region 2   │   │   Region 3   │      │
│  │  Slot 0–99   │   │ Slot 100–199 │   │ Slot 200–299 │      │
│  │              │   │              │   │              │      │
│  │  Raft Group 1│   │  Raft Group 2│   │  Raft Group 3│      │
│  │  Leader: N1  │   │  Leader: N2  │   │  Leader: N3  │      │
│  └──────────────┘   └──────────────┘   └──────────────┘      │
└──────────────────────────────────────────────────────────────┘
```

和我们之前的固定 Hash 分片相比，Multi-Raft 的 Region 有几个本质区别。Region 的边界是动态的——当一个 Region 的数据量增长到阈值时，它可以自动分裂成两个 Region，不需要人工干预。不同 Region 的 Leader 天然分布在不同节点上，写入负载自动分散，不需要外部的代理层来做路由。每个 Region 是一个独立的故障域，一个 Region 的 Leader 故障只影响该 Region 对应的 Slot 范围，其他 Region 不受影响。

当然，Multi-Raft 也带来了新的工程挑战：数百个 Raft 组共享一个 RocksDB 实例，如何避免 I/O 放大？谁来决定 Region 的分裂、合并、Leader 迁移？跨 Region 的操作（比如 MGET 涉及多个 Slot）怎么处理？这些问题正是 NeoKV 要解决的，我们将在后续章节中逐一展开。

## NeoKV 中的 Raft

最后，简单总结一下 Raft 在 NeoKV 中扮演的角色：

每个 Region 是一个 Raft 组。Region 管理一段连续的 Redis Slot 范围，三副本分布在不同 Store 节点上。所有 Redis 写命令（SET、DEL、HSET 等）被序列化为 protobuf 消息，通过 Raft 日志复制到多数节点后才执行。读命令可以不经过 Raft——Leader 直接读本地 RocksDB，Follower 通过 ReadIndex 协议保证读到最新数据。MetaServer 自身也是一个 Raft 组，集群元数据的变更也通过 Raft 保证一致性。

NeoKV 使用百度开源的 [braft](https://github.com/baidu/braft) 作为 Raft 实现。braft 是一个成熟的 C++ Raft 库，基于 brpc 框架，提供了 Leader 选举、日志复制、快照、成员变更等完整功能。但直接使用 braft 在 Multi-Raft 场景下会遇到一些实际问题，我们在下一章详细讨论。

## 检验你的理解

- Raft 如何保证已 committed 的日志不会丢失？如果 Leader 在 commit 之后、回复 Client 之前宕机了，会发生什么？
- 为什么 Raft 要求"多数节点确认"而不是"所有节点确认"？3 节点集群和 5 节点集群分别能容忍几个节点故障？
- Multi-Raft 中，不同 Region 的 Leader 可以在同一个物理节点上吗？这对负载均衡有什么影响？
- 对比 NeoKV 和之前描述的 Codis + Redis 集群，在故障切换场景下，两者的行为有什么本质区别？

## 延伸阅读

- [Raft 论文](https://raft.github.io/raft.pdf) — 原始论文，强烈推荐阅读
- [Raft 可视化](https://raft.github.io/) — 交互式动画，帮助理解选举和日志复制
- [braft 文档](https://github.com/baidu/braft/blob/master/docs/cn/raft_protocol.md) — braft 对 Raft 协议的中文说明

---

> 下一章：[02-braft 实践](./02-braft实践.md) — 我们将深入分析 braft 框架，以及 NeoKV 在其上做了哪些关键定制。
