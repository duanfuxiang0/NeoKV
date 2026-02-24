# Raft 共识算法

## 概览

- 为什么需要共识算法
- Raft 的核心机制：Leader 选举、日志复制、安全性保证
- 从单 Raft 组到 Multi-Raft 的演进思路
- 为什么选择学习 Raft 作为共识协议

如果你已经熟悉 Raft，可以直接跳到 [02-braft 实践](./02-braft实践.md)。

## 1. 为什么需要共识算法

假设我们有一个单机 KV 存储（比如一个 RocksDB 实例）。它工作得很好，但有一个致命问题：**单点故障**。机器一旦宕机，数据就不可用了。

最直觉的解决方案是**复制**——把数据复制到多台机器上。但复制引入了新问题：

- **如何保证多个副本的数据一致？** 如果 Client A 写入 `x=1`，Client B 同时写入 `x=2`，不同副本可能看到不同的值。
- **如何处理网络分区？** 如果部分节点之间网络断开，各自继续服务，数据就会分裂（split-brain）。
- **如何处理节点故障？** 如果持有最新数据的节点宕机了，其他节点如何知道哪些数据是"已确认"的？

这就是 **共识问题（Consensus Problem）**：让一组节点就某个值（或一系列操作的顺序）达成一致，即使部分节点发生故障。

### 1.1 CAP 定理与一致性模型

CAP 定理告诉我们，分布式系统在网络分区（P）发生时，只能在一致性（C）和可用性（A）之间二选一：

| 系统 | 选择 | 代价 |
|------|------|------|
| Redis Sentinel / Kvrocks | AP（可用性优先） | 异步复制，主从切换可能丢数据 |
| NeoKV / TiKV / CockroachDB | CP（一致性优先） | 写入需要多数节点确认，延迟更高 |

NeoKV 选择了 **CP**——强一致性。每次写入都必须经过多数节点确认（Raft 共识），确保不会丢失已确认的数据。这个选择的代价是写入延迟更高（需要网络往返），但换来的是更强的数据保证。

### 1.2 Paxos vs Raft

在 Raft 之前，Paxos 是最著名的共识算法。但 Paxos 有一个广为人知的问题：**太难理解了**。Lamport 的原始论文用希腊议会的比喻来描述算法，连很多分布式系统专家都承认难以完全理解。

2014 年，Diego Ongaro 和 John Ousterhout 发表了 [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)，提出了 Raft。Raft 的核心创新不在于算法本身的能力（它与 Multi-Paxos 等价），而在于 **可理解性**：

- **强 Leader**：所有写入都通过 Leader，简化了并发控制
- **日志连续**：日志条目按顺序追加，不允许空洞
- **成员变更**：提供了安全的集群成员变更机制

这些设计让 Raft 成为工业界最流行的共识算法实现基础。TiKV、CockroachDB、etcd 都基于 Raft。

## 2. Raft 核心机制

Raft 将共识问题分解为三个相对独立的子问题：

### 2.1 Leader 选举

Raft 集群中的每个节点处于三种状态之一：

```
                超时，发起选举
    ┌─────────────────────────────┐
    │                             ▼
 Follower ◄──────────────── Candidate
    ▲          选举失败            │
    │          或发现新 Leader      │ 获得多数票
    │                             ▼
    └──────────────────────── Leader
          发现更高 Term
```

- **Follower**：被动接收 Leader 的日志和心跳。如果超时未收到心跳，转为 Candidate。
- **Candidate**：发起选举，向其他节点请求投票（RequestVote RPC）。
- **Leader**：处理所有客户端请求，向 Follower 复制日志。

**Term（任期）** 是 Raft 的逻辑时钟。每次选举开始时 Term 递增。一个 Term 内最多只有一个 Leader。如果节点收到更高 Term 的消息，它会立即退回 Follower 状态。

选举的关键规则：
- 每个节点在一个 Term 内只能投一票（先到先得）
- Candidate 必须获得 **多数节点** 的投票才能成为 Leader
- 投票时会检查候选人的日志是否"至少和自己一样新"（Election Restriction）

### 2.2 日志复制

Leader 选出后，所有写操作都通过 Leader 处理：

```
Client          Leader           Follower1        Follower2
  │               │                 │                │
  │──SET x=1────►│                 │                │
  │               │──AppendEntries─►│                │
  │               │──AppendEntries──┼───────────────►│
  │               │                 │                │
  │               │◄─── ACK ────────│                │
  │               │    (多数确认)    │                │
  │               │                 │                │
  │               │  commit index++ │                │
  │               │  apply to SM    │                │
  │◄──── OK ──────│                 │                │
  │               │                 │                │
  │               │──心跳(含新commit)►│               │
  │               │                 │  apply to SM   │
```

1. Client 发送写请求到 Leader
2. Leader 将操作追加到本地日志，然后并行发送 AppendEntries RPC 给所有 Follower
3. 当 **多数节点**（含 Leader 自己）确认收到日志后，该条目被标记为 **committed**
4. Leader 将 committed 的日志应用到状态机（State Machine），返回结果给 Client
5. Follower 在后续心跳中得知新的 commit index，也将日志应用到自己的状态机

这就是 Raft 的核心保证：**一旦一条日志被 committed，它就不会丢失**（只要多数节点存活）。

### 2.3 安全性保证

Raft 通过以下机制保证安全性：

- **Leader Completeness**：如果一条日志在某个 Term 被 committed，那么所有更高 Term 的 Leader 都包含这条日志。这通过选举时的日志比较（Election Restriction）保证。

- **State Machine Safety**：如果某个节点在某个 index 应用了一条日志，那么其他节点在同一 index 不会应用不同的日志。

- **Log Matching**：如果两个节点的日志在某个 index 处的 Term 相同，那么它们在该 index 及之前的所有日志都相同。

这些性质共同保证了：**所有节点的状态机最终会以相同的顺序执行相同的操作**。

## 3. 从单 Raft 到 Multi-Raft

单个 Raft 组有明显的局限性：

- **写入瓶颈**：所有写入都经过单一 Leader，无法水平扩展
- **数据量限制**：单个 Raft 组的数据量受限于单机存储容量
- **热点问题**：所有请求集中在一个 Leader 上

解决方案是 **Multi-Raft**：将数据分成多个分片（NeoKV 中叫 Region），每个分片是一个独立的 Raft 组。

```
┌─────────────────────────────────────────────────┐
│                   数据空间                        │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │ Region 1 │  │ Region 2 │  │ Region 3 │ ...  │
│  │ Slot 0-99│  │Slot 100- │  │Slot 200- │      │
│  │          │  │   199    │  │   299    │      │
│  │ Raft 组1 │  │ Raft 组2 │  │ Raft 组3 │      │
│  └──────────┘  └──────────┘  └──────────┘      │
│                                                  │
│  每个 Region 独立选举 Leader，独立复制日志          │
│  不同 Region 的 Leader 可以分布在不同节点上         │
└─────────────────────────────────────────────────┘
```

Multi-Raft 的优势：

- **写入可扩展**：不同 Region 的 Leader 分布在不同节点，写入负载分散
- **数据可扩展**：Region 可以分裂（Split），数据量不受单机限制
- **故障隔离**：一个 Region 的 Leader 故障只影响该 Region，其他 Region 不受影响

但 Multi-Raft 也带来了新的工程挑战：

- **数百个 Raft 组共享一个存储引擎**：如何避免 I/O 放大？
- **需要全局调度器**：谁来决定 Region 的分裂、合并、Leader 迁移？
- **跨 Region 操作**：如何处理涉及多个 Region 的请求？

这些问题正是 NeoKV 要解决的，我们将在后续章节中逐一展开。

## 4. NeoKV 中的 Raft

在 NeoKV 中，Raft 的角色是：

- **每个 Region 是一个 Raft 组**：Region 管理一段连续的 Redis Slot 范围，三副本分布在不同 Store 节点上
- **所有 Redis 写命令经过 Raft**：SET、DEL、HSET 等写操作被序列化为 protobuf 消息，通过 Raft 日志复制到多数节点后才执行
- **读命令可以不经过 Raft**：Leader 直接读本地 RocksDB；Follower 通过 ReadIndex 协议保证读到最新数据
- **MetaServer 自身也是一个 Raft 组**：集群元数据的变更也通过 Raft 保证一致性

NeoKV 使用百度开源的 [braft](https://github.com/baidu/braft) 作为 Raft 实现。braft 是一个成熟的 C++ Raft 库，基于 brpc 框架，提供了 Leader 选举、日志复制、快照、成员变更等完整功能。但直接使用 braft 在 Multi-Raft 场景下会遇到一些实际问题，我们在下一章详细讨论。

## 检验你的理解

- Raft 如何保证已 committed 的日志不会丢失？如果 Leader 在 commit 之后、回复 Client 之前宕机了，会发生什么？
- 为什么 Raft 要求"多数节点确认"而不是"所有节点确认"？3 节点集群和 5 节点集群分别能容忍几个节点故障？
- Multi-Raft 中，不同 Region 的 Leader 可以在同一个物理节点上吗？这对负载均衡有什么影响？
- NeoKV 的读操作为什么可以不经过 Raft？这样做有什么前提条件？

## 延伸阅读

- [Raft 论文](https://raft.github.io/raft.pdf) — 原始论文，强烈推荐阅读
- [Raft 可视化](https://raft.github.io/) — 交互式动画，帮助理解选举和日志复制
- [braft 文档](https://github.com/baidu/braft/blob/master/docs/cn/raft_protocol.md) — braft 对 Raft 协议的中文说明

---

> 下一章：[02-braft 实践](./02-braft实践.md) — 我们将深入分析 braft 框架，以及 NeoKV 在其上做了哪些关键定制。
