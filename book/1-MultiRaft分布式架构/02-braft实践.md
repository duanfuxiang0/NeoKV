# braft 实践

## 概览

- braft 框架的核心抽象和使用方式
- 通过 Counter 示例理解 braft 的完整工作流程
- 提交任务、Closure 回调、状态监听、Snapshot 机制
- 使用 braft 时的常见陷阱和实践建议
- braft 的可插拔存储架构

如果你已经熟悉 braft，可以直接跳到 [03-Multi-Raft 与 Region](./03-Multi-Raft与Region.md)。

## 选择 braft 的背景

上一章我们聊了 Raft 的理论，也聊了为什么 NeoKV 需要一个共识算法来替代 Redis Sentinel 那套异步复制。理论讲完了，接下来的问题很现实：我们要自己从零实现一个 Raft 库，还是站在别人的肩膀上？

自己写 Raft 听起来很酷，但实际上是个坑。Raft 论文读起来清晰易懂，但从论文到生产级实现之间隔着大量的工程细节——预投票（PreVote）防止网络分区后的 Term 膨胀、Pipeline 和 Batch 优化吞吐、快照的流式传输和断点续传、成员变更的 Joint Consensus……这些东西论文里要么一笔带过，要么根本没提。如果你在生产环境里踩过 Raft 的坑，你就知道这些"细节"中的任何一个都可能让你的集群在凌晨三点出问题。

所以我们选择了百度开源的 [braft](https://github.com/baidu/braft)。braft 是一个成熟的 C++ Raft 库，基于 brpc 框架，在百度内部经过了大规模生产验证。它提供了完整的 Raft 协议实现——Leader 选举、日志复制、快照、成员变更——而且关键的是，它的存储层是可插拔的。这一点对 NeoKV 至关重要，后面你会看到为什么。

## braft 的核心抽象

braft 的设计围绕一个简单的思路：它帮你处理所有 Raft 协议层面的事情（选举、日志复制、心跳、成员变更），你只需要告诉它两件事——"数据怎么存"和"日志 committed 之后做什么"。

整体结构大概长这样：

```
┌──────────────────────────────────────────────────────┐
│                     braft::Node                      │
│                                                      │
│  ┌────────────────┐   ┌──────────────────────────┐   │
│  │  Raft Protocol │   │   Pluggable Storage      │   │
│  │  Election      │   │                          │   │
│  │  Replication   │   │  LogStorage (日志)        │   │
│  │                │   │  RaftMetaStorage (元数据)  │   │
│  │                │   │  SnapshotStorage (快照)   │   │
│  └────────────────┘   └──────────────────────────┘   │
│          │                                           │
│          ▼                                           │
│  ┌───────────────┐                                   │
│  │  StateMachine │  ← 用户实现                        │
│  │   on_apply()  │                                   │
│  │   on_snapshot │                                   │
│  └───────────────┘                                   │
└──────────────────────────────────────────────────────┘
```

左边是 Raft 协议引擎，你不需要碰它。右边是三个可插拔的存储后端，braft 提供了基于本地文件系统的默认实现（URI 前缀 `local://`），但你可以替换成自己的。下面的 StateMachine 是你必须实现的核心接口。

braft 需要三种不同的持久存储：RaftMetaStorage 用来存放 Raft 算法自身的状态数据（term、vote_for 等），LogStorage 用来存放用户提交的 WAL，SnapshotStorage 用来存放用户的 Snapshot 以及元信息。它们通过 URI 来配置，比如 `local://data` 就是存放到当前目录的 data 文件夹。

这些概念说起来有点抽象，我们直接看一个完整的例子。

## 从 Counter 示例说起

braft 官方提供了一个 Counter 示例（`example/counter/server.cpp`），它实现了一个分布式计数器——多个节点通过 Raft 共识保证计数值的一致性。这个例子虽然简单，但它覆盖了使用 braft 的所有关键步骤。我们一步步拆解。

**第一步：启动 brpc Server 并注册 braft 服务。** braft 本身不提供 server 功能，它需要运行在 brpc server 里面。你可以让 braft 和你的业务共享同一个端口，也可以分开。Counter 示例选择了共享：

```cpp
int main(int argc, char* argv[]) {
    brpc::Server server;
    example::Counter counter;
    example::CounterServiceImpl service(&counter);

    // 先注册你的业务 Service
    server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE);

    // 再注册 braft 的内部 Service（选举、日志复制等 RPC）
    braft::add_service(&server, FLAGS_port);

    // 先启动 server，再启动 Raft 节点
    // 这个顺序很重要——避免节点成为 Leader 时服务还不可达
    server.Start(FLAGS_port, NULL);
    counter.start();
    // ...
}
```

这里有一个容易忽略的细节：**必须先启动 server，再启动 Raft 节点**。如果反过来，节点可能在 server 还没 ready 的时候就赢得选举成为 Leader，这时候客户端连不上来，会造成一段时间的服务不可用。另外，`add_service` 传入的端口必须和 `server.Start` 的端口一致，否则节点之间的 RPC 会找不到对方。

**第二步：构造 braft::Node 并初始化。** 一个 Node 代表一个 Raft 实例，它的 ID 由 GroupId（复制组名称）和 PeerId（节点地址 + index）组成：

```cpp
int Counter::start() {
    butil::EndPoint addr(butil::my_ip(), FLAGS_port);
    braft::NodeOptions node_options;

    // 初始集群配置，只在空节点首次启动时生效
    node_options.initial_conf.parse_from(FLAGS_conf);
    node_options.election_timeout_ms = FLAGS_election_timeout_ms;
    node_options.fsm = this;  // Counter 自身就是状态机
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = FLAGS_snapshot_interval;

    // 三种存储的 URI 配置
    std::string prefix = "local://" + FLAGS_data_path;
    node_options.log_uri = prefix + "/log";
    node_options.raft_meta_uri = prefix + "/raft_meta";
    node_options.snapshot_uri = prefix + "/snapshot";

    braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
    node->init(node_options);
    _node = node;
    return 0;
}
```

`initial_conf` 是一个关键参数。它只在集群从空节点启动时生效——如果节点已经有 snapshot 或 log 数据，braft 会从中恢复 Configuration，忽略 `initial_conf`。创建集群时，你可以让所有节点设置相同的 `initial_conf`（包含所有节点的 ip:port）来同时启动，也可以先启动一个节点，再通过 `add_peer` 逐个添加其他节点。

**第三步：实现 StateMachine。** 这是你跟 braft 打交道最多的地方。Counter 示例的状态机非常简洁——状态就是一个 `int64_t` 的计数值：

```cpp
class Counter : public braft::StateMachine {
    braft::Node* volatile _node;
    butil::atomic<int64_t> _value;        // 这就是"状态"
    butil::atomic<int64_t> _leader_term;  // 当前 Leader 的 term，-1 表示非 Leader

    void on_apply(braft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            int64_t delta_value = 0;
            CounterResponse* response = NULL;
            braft::AsyncClosureGuard closure_guard(iter.done());

            if (iter.done()) {
                // 这条日志是本节点（Leader）提交的
                // 可以直接从 Closure 中拿到请求，避免重复解析
                FetchAddClosure* c = dynamic_cast<FetchAddClosure*>(iter.done());
                response = c->response();
                delta_value = c->request()->value();
            } else {
                // 这条日志是从 Leader 复制过来的（Follower）
                // 需要从 data 中反序列化
                FetchAddRequest request;
                request.ParseFromZeroCopyStream(&wrapper);
                delta_value = request.value();
            }

            // 更新状态机
            const int64_t prev = _value.fetch_add(delta_value);
            if (response) {
                response->set_success(true);
                response->set_value(prev);
            }
        }
    }
};
```

这段代码里有几个值得注意的地方。

`on_apply` 是批量调用的——通过 `braft::Iterator` 你可以一次遍历多条已 committed 的日志。如果你的状态机支持批量更新（比如 RocksDB 的 WriteBatch），可以一次性处理多条日志来提高吞吐。

`iter.done()` 的返回值区分了两种情况：如果非 NULL，说明这条日志是当前节点作为 Leader 提交的，`done()` 就是 `apply()` 时传入的 Closure，你可以从中直接拿到原始请求和响应对象，避免重复反序列化。如果是 NULL，说明这条日志是从 Leader 复制过来的（当前节点是 Follower），你需要从 `iter.data()` 中解析请求。

`AsyncClosureGuard` 是一个 RAII 辅助类，它保证在作用域结束时自动调用 `iter.done()->Run()`。这很重要——**无论操作成功还是失败，你都必须调用 `done()->Run()`**，否则对应的资源会泄漏。用 Guard 可以避免忘记调用或者异常路径遗漏。

**第四步：提交写操作到 Raft。** 客户端的写请求不能直接修改状态机，必须通过 Raft 共识。Counter 示例的 `fetch_add` 方法展示了标准流程：

```cpp
void Counter::fetch_add(const FetchAddRequest* request,
                        CounterResponse* response,
                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    // 1. 检查是否是 Leader
    const int64_t term = _leader_term.load(butil::memory_order_relaxed);
    if (term < 0) {
        return redirect(response);  // 不是 Leader，重定向
    }

    // 2. 序列化请求到 IOBuf
    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream wrapper(&log);
    request->SerializeToZeroCopyStream(&wrapper);

    // 3. 构造 Task 并提交
    braft::Task task;
    task.data = &log;
    task.done = new FetchAddClosure(this, request, response,
                                    done_guard.release());
    task.expected_term = term;  // 防止 ABA 问题

    _node->apply(task);
}
```

这里有几个关键点。`apply()` 是异步的——它立即返回，日志复制和应用在后台进行。当日志被 committed 并 apply 到状态机后，`task.done`（也就是 `FetchAddClosure`）的 `Run()` 方法会被调用，在那里你完成对客户端的响应。

`apply()` 是线程安全的，而且实现接近 wait-free，你可以从多个线程同时向同一个 Node 提交任务。

`expected_term` 是一个容易被忽略但很重要的参数。想象这个场景：节点在 term 1 是 Leader，提交了一条日志，但还没来得及处理就发生了主从切换，很短时间内这个节点又变成了 term 3 的 Leader，之前那条日志才开始被处理。如果不检查 term，这条"旧 Leader 时代"的日志可能会被"新 Leader"错误地 apply。设置 `expected_term` 可以让 braft 在 term 不匹配时拒绝这条任务，避免 ABA 问题。

**第五步：实现 Closure 回调。** `braft::Closure` 是 braft 通知你异步操作结果的机制。Counter 示例的 `FetchAddClosure` 长这样：

```cpp
class FetchAddClosure : public braft::Closure {
public:
    void Run() {
        std::unique_ptr<FetchAddClosure> self_guard(this);
        brpc::ClosureGuard done_guard(_done);
        if (status().ok()) {
            return;  // 成功，response 已经在 on_apply 中设置好了
        }
        // 失败，尝试重定向到新 Leader
        _counter->redirect(_response);
    }
};
```

`status()` 告诉你这次操作是否成功。如果失败了（比如 Leader 切换、节点 shutdown），你需要处理错误——通常是告诉客户端去找新 Leader。

这里有一个 braft 文档里特别强调的点：**apply 的结果存在 false negative**。也就是说，框架告诉你"写失败了"，但实际上相同内容的日志可能已经被新 Leader 确认提交并 apply 到了状态机。这在 Leader 切换的窗口期会发生。客户端收到失败通常会重试，所以你的操作最好是幂等的——同一个操作执行两次和执行一次的效果应该一样。

**第六步：监听 Leader 状态变更。** Counter 示例用一个原子变量 `_leader_term` 来追踪当前节点是否是 Leader：

```cpp
void Counter::on_leader_start(int64_t term) {
    _leader_term.store(term, butil::memory_order_release);
    LOG(INFO) << "Node becomes leader";
}

void Counter::on_leader_stop(const butil::Status& status) {
    _leader_term.store(-1, butil::memory_order_release);
    LOG(INFO) << "Node stepped down : " << status;
}

bool Counter::is_leader() const {
    return _leader_term.load(butil::memory_order_acquire) > 0;
}
```

这个模式很简洁：`_leader_term > 0` 表示当前是 Leader，`-1` 表示不是。写操作在提交前检查这个值，如果不是 Leader 就重定向。

除了 `on_leader_start/stop`，StateMachine 还提供了其他状态回调：`on_shutdown`（节点关闭）、`on_error`（严重错误，节点不再工作）、`on_configuration_committed`（配置变更提交）、`on_start_following/on_stop_following`（开始/停止跟随某个 Leader）。在生产环境中，你通常需要在这些回调里做一些事情——比如在 `on_error` 中触发告警，在 `on_configuration_committed` 中更新本地的节点列表缓存。

**第七步：处理读操作。** Counter 示例的读操作很直接——只在 Leader 上读：

```cpp
void Counter::get(CounterResponse* response) {
    if (!is_leader()) {
        return redirect(response);
    }
    response->set_success(true);
    response->set_value(_value.load(butil::memory_order_relaxed));
}
```

这是最简单的读取方式，但严格来说它有一个问题：在网络分区的极端情况下，旧 Leader 可能不知道自己已经被取代，仍然认为自己是 Leader 并返回过期数据。对于 Counter 这种示例来说无所谓，但对于需要强一致读的场景，你需要用 ReadIndex 或 Lease Read 来保证。NeoKV 中我们两种都支持，具体实现在下一章讨论。

**第八步：实现 Snapshot。** Snapshot 有两个作用：启动加速（不需要从头回放所有日志）和 Log Compaction（snapshot 之前的日志可以删除）。Counter 示例的 snapshot 就是把当前计数值序列化到一个文件里：

```cpp
void Counter::on_snapshot_save(braft::SnapshotWriter* writer,
                               braft::Closure* done) {
    // 在 bthread 中异步执行，避免阻塞状态机
    SnapshotArg* arg = new SnapshotArg;
    arg->value = _value.load(butil::memory_order_relaxed);
    arg->writer = writer;
    arg->done = done;
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

static void* save_snapshot(void* arg) {
    SnapshotArg* sa = (SnapshotArg*)arg;
    brpc::ClosureGuard done_guard(sa->done);

    // 1. 将状态序列化到文件
    std::string snapshot_path = sa->writer->get_path() + "/data";
    Snapshot s;
    s.set_value(sa->value);
    braft::ProtoBufFile pb_file(snapshot_path);
    pb_file.save(&s, true);

    // 2. 将文件注册到 snapshot meta
    sa->writer->add_file("data");
    return NULL;
}

int Counter::on_snapshot_load(braft::SnapshotReader* reader) {
    // Leader 不应该加载 snapshot
    CHECK(!is_leader());

    std::string snapshot_path = reader->get_path() + "/data";
    braft::ProtoBufFile pb_file(snapshot_path);
    Snapshot s;
    pb_file.load(&s);
    _value.store(s.value(), butil::memory_order_relaxed);
    return 0;
}
```

注意 `on_snapshot_save` 是在单独的 bthread 中执行的。这是因为 snapshot 可能涉及大量 I/O，如果在状态机线程中同步执行，会阻塞后续日志的 apply。对于内存数据量小的场景（比如 Counter），你可以先把数据拷贝出来，然后在后台线程中持久化。对于使用 RocksDB 这类支持 MVCC 的存储引擎，你可以先创建一个 db snapshot，然后在后台线程中遍历并持久化——TiKV、CockroachDB 都是这个思路。

## 实践中的陷阱

Counter 示例虽然清晰，但它毕竟是一个教学用的 demo。在真实项目中使用 braft，有几个坑是你几乎一定会踩到的。

**操作必须幂等。** 前面提到过，apply 存在 false negative——框架说失败了，但日志可能已经被提交。客户端重试时，同一个操作可能被执行两次。如果你的操作是 `SET key value`，执行两次没问题；但如果是 `INCR counter`，执行两次就多加了一次。解决方案通常是给每个请求带一个唯一 ID，状态机在 apply 时检查是否已经处理过。

**不同日志的处理结果是独立的。** 一个线程连续提交了 A、B 两条日志，以下组合都可能发生：A 和 B 都成功、A 和 B 都失败、A 成功 B 失败、A 失败 B 成功。只有当 A 和 B 都成功时，它们在日志中的顺序才保证和提交顺序一致。这意味着你不能假设"A 成功了 B 就一定成功"。

**on_apply 中不要做耗时操作。** `on_apply` 是串行调用的——一条日志的 apply 会阻塞后续所有日志。如果你在 `on_apply` 里做了一次远程 RPC 或者一次很慢的磁盘操作，整个状态机的吞吐都会被拖垮。正确的做法是让 `on_apply` 尽可能快地完成，耗时操作放到异步线程中。

**节点配置变更要小心。** braft 提供了 `add_peer`、`remove_peer`、`change_peers` 来动态调整集群成员。变更过程分为追赶阶段（新节点追数据）、联合选举阶段（新旧配置共同决策）和新配置同步阶段。在追赶阶段完成前，新节点不会被计入决策集合，不影响数据安全。但有一个危险操作叫 `reset_peers`——它在多数节点故障时强制重置配置，不经过 Raft 共识，可能导致脑裂。除非你明确知道自己在做什么，否则不要用它。

**Leader Transfer 的时序。** 有时候你需要主动迁移 Leader——比如主节点要重启，或者你想把 Leader 迁移到离客户端更近的节点。braft 的 `transfer_leadership_to` 实现了这个功能：Leader 先停止接受写入，等目标节点的日志追上后发送 TimeoutNow RPC 触发它立即选举，然后自己 step down。整个过程中有一个 `election_timeout_ms` 的超时保护——如果超时内没有完成迁移，Leader 会取消操作重新接受写入。

## 节点状态监控

braft 在启动后会在 `http://${your_server_endpoint}/raft_stat` 上暴露节点状态，这在排查问题时非常有用。你可以看到当前节点的角色（Leader/Follower/Candidate）、term、已提交的日志 index、已 apply 的日志 index、snapshot 状态等。如果你发现 `last_committed_index` 和 `known_applied_index` 之间差距很大，说明 apply 跟不上 commit 的速度，状态机可能有性能瓶颈。

braft 还提供了丰富的 flags 配置项，运行时可以通过 `http://endpoint/flags` 查看和修改。几个比较重要的：

- `raft_sync`：是否开启 sync，关闭可以提升性能但牺牲持久性
- `raft_max_segment_size`：单个 log segment 文件大小
- `raft_max_entries_size`：AppendEntries 包含的最大 entry 数量
- `raft_apply_batch`：apply 时的最大 batch 数量
- `raft_election_heartbeat_factor`：选举超时与心跳超时的比例

## braft 的可插拔存储

回到我们最开始提到的那个关键特性：braft 的存储层是可插拔的。

braft 默认提供了基于本地文件系统的存储实现，URI 前缀是 `local://`。但它允许你通过 `RegisterOrDie` 注册自定义的存储后端，只要 URI 前缀不同，braft 就会自动选择对应的实现：

```cpp
// 注册自定义存储后端
braft::log_storage_extension()->RegisterOrDie("myraftlog", &my_log_instance);
braft::meta_storage_extension()->RegisterOrDie("myraftmeta", &my_meta_instance);

// 使用时只需要换 URI 前缀
options.log_uri = "myraftlog://my_raft_log?id=1";
options.raft_meta_uri = "myraftmeta://my_raft_meta?id=1";
```

这种设计本质上是一个工厂模式——URI 前缀是 key，存储实现是 value。braft 的协议层完全不关心数据存在哪里，你可以自由地优化存储层而不影响 Raft 协议的正确性。

对于单 Raft 组的场景，默认的 `local://` 实现完全够用。但当你在一台机器上运行数百个 Raft 组时——这正是 NeoKV 的 Multi-Raft 架构——默认实现会遇到严重的 I/O 问题。数百个目录、上千个文件描述符、每个 Raft 组独立 fsync……这些问题迫使我们实现了自定义的存储后端，将所有 Raft 组的日志和元数据汇聚到同一个 RocksDB 实例中。

这正是下一章的主题。

## 检验你的理解

- braft 的 `apply()` 是异步的，那如何实现"客户端同步等待写入结果"？Counter 示例用了什么模式？
- 为什么 `on_apply` 中需要区分 `iter.done()` 是否为 NULL？这两种情况分别对应什么场景？
- `expected_term` 解决的是什么问题？如果不设置它，在什么场景下会出错？
- Snapshot 的 `on_snapshot_save` 为什么要在单独的 bthread 中执行？如果同步执行会有什么后果？
- `reset_peers` 为什么危险？在什么情况下你不得不使用它？

## 延伸阅读

- [braft server 端文档](https://github.com/baidu/braft/blob/master/docs/cn/server.md) — 官方的 server 端使用指南
- [braft Counter 示例](https://github.com/baidu/braft/tree/master/example/counter) — 本章讲解的完整源码
- [braft CLI 工具](https://github.com/baidu/braft/blob/master/docs/cn/cli.md) — 通过命令行控制 Raft 节点

---

> 下一章：[03-Multi-Raft 与 Region](./03-Multi-Raft与Region.md) — 我们将深入 NeoKV 如何在 braft 之上构建 Multi-Raft 架构，包括自定义存储后端、Region 作为 Raft 状态机的完整生命周期，以及 Store 如何管理数百个 Region。
