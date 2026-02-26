# RESP 协议与命令框架

## 概览

- brpc 的 RedisService：NeoKV 如何零成本接入 RESP 协议
- 命令注册与 Handler 生命周期
- RedisDataCommandHandler 基类：路由、一致性、读写分发
- 写命令链路：Handler → Raft → Apply → Response
- 读命令链路：路由 → 一致性检查 → RocksDB 直读
- 返回值传递的工程妥协
- Standalone 模式：单进程测试服务器

在前面的章节中，我们详细讨论了 NeoKV 的存储编码和五种 Redis 数据类型的实现。但这些实现最终需要通过网络暴露给 Redis 客户端。一个自然的问题是：NeoKV 是如何处理 Redis 协议的？它自己实现了 RESP 解析吗？命令是如何从客户端的 `SET key value` 一路到达 RocksDB 的 `WriteBatch.Put()` 的？

答案出人意料的简洁——NeoKV 没有写一行 RESP 协议解析代码。

## brpc 内置的 Redis 协议支持

NeoKV 使用的 RPC 框架 brpc 内置了完整的 Redis 协议支持。`brpc::RedisService` 提供了 RESP 协议解析、命令分发、响应编码和连接管理——我们只需要实现每个命令的业务逻辑。brpc 定义了一个抽象接口 `brpc::RedisCommandHandler`，核心方法是 `Run()`，它接收解析好的命令参数（`args[0]` 是命令名，`args[1..]` 是参数），输出写入 `brpc::RedisReply`。

服务启动时，NeoKV 创建一个 `brpc::RedisService` 实例，调用 `register_basic_redis_commands()` 注册所有命令 Handler，然后将 service 设置到 `brpc::ServerOptions` 的 `redis_service` 字段上，启动 server 监听 `FLAGS_redis_port`。整个启动流程不到 20 行代码。`neo_redis_standalone.cpp` 中的实现清楚地展示了这一点——创建 `brpc::RedisService`，注册命令，设置到 server options，start。没有 socket 编程，没有协议解析，没有连接池。

这个架构选择意味着 NeoKV 的 Redis 协议兼容性完全取决于 brpc 的 RESP 实现。如果未来需要支持 RESP3（Redis 6.0 引入的新协议），需要 brpc 先支持，NeoKV 本身不需要改动。这是一个合理的依赖——brpc 是一个成熟的工业级框架，RESP 协议本身也足够稳定。

## 命令注册

`register_basic_redis_commands()`（`src/redis/redis_service.cpp`）是所有命令的注册入口。它使用一个内部的 `add_handler()` 辅助函数，接收一个 `std::unique_ptr<brpc::RedisCommandHandler>` 和一组命令名：

```cpp
bool add_handler(brpc::RedisService* service,
                 std::unique_ptr<brpc::RedisCommandHandler> handler,
                 std::initializer_list<const char*> names) {
    brpc::RedisCommandHandler* raw_handler = handler.get();
    for (const auto* name : names) {
        if (!service->AddCommandHandler(name, raw_handler)) {
            return false;
        }
    }
    command_holders().push_back(std::move(handler));
    return true;
}
```

Handler 的所有权转移给一个静态的 `command_holders()` vector，与进程同生命周期。这意味着每个 Handler 只创建一次，所有请求共享同一个 Handler 实例——Handler 必须是无状态的（所有状态通过参数传入）。一个 Handler 可以注册多个命令名，比如 `FlushdbCommandHandler` 同时注册了 `"flushdb"` 和 `"flushall"`。

注册分为多个阶段。**Phase 0** 是基础命令（`ping`、`echo`、`cluster`），无需完整的 NeoKV 集成即可工作。**Phase 1** 及之后的数据命令被 `#ifdef NEOKV_REDIS_FULL` 条件编译包裹——这让 NeoKV 可以编译出一个只支持基础命令的精简版本（虽然实际部署中几乎不会这样做）。Phase 1 注册核心命令（GET/SET/DEL/MGET/MSET/EXPIRE/TTL），Phase 2 扩展读写命令（EXISTS/PTTL/STRLEN/TYPE/SETNX 等），后续阶段依次注册 Hash、Set、List、ZSet 和其他类型命令。

每个阶段注册完成后会输出一行日志，比如 `"Redis Phase 1 commands registered: GET/SET/DEL/MGET/MSET/EXPIRE/TTL"`。这在排查启动问题时很有帮助——如果某个命令没有注册成功，日志中会有对应的 `DB_FATAL`。

## RedisDataCommandHandler：数据命令的基类

绝大多数 Redis 数据命令都继承自 `RedisDataCommandHandler`（定义在 `src/redis/redis_service.cpp` 内部，不在头文件中暴露）。这个基类提取了三个核心操作：**路由**、**一致性检查**和**Raft 写入**。

**`check_route()`** 是每个命令的第一步。它调用 `RedisRouter::route(args)` 来确定目标 Region。route 的过程包括：从命令参数中提取所有 key（根据 `RedisKeyPattern`，下一章详述），计算 slot，检查多 key 是否在同一个 slot（CROSSSLOT 校验），然后在 SlotTable 中查找 Region。返回的 `RedisRouteResult` 包含状态码（OK、CROSSSLOT、CLUSTERDOWN、NOT_LEADER、INVALID_CMD）、slot 号、region_id、以及 Region 的智能指针。如果路由失败，`check_route()` 直接设置错误响应并返回 `false`，调用方只需要检查返回值就知道是否需要继续。

**`check_read_consistency()`** 在读命令路由成功后调用，决定当前节点是否能提供一致性读。这个方法的逻辑严格对齐 SQL 引擎的 `Region::query()` 实现——Leader 直接读；Follower 或 Learner 在启用 `FLAGS_use_read_index` 时通过 ReadIndex 机制确保线性一致性（调用 `region->follower_read_wait()`，超时由 `FLAGS_follow_read_timeout_s` 控制，默认 10 秒）；Learner 还需要额外检查 `learner_ready_for_read()`，防止读到正在做 Snapshot 恢复的 Learner 上的陈旧数据；如果都不满足，返回 MOVED 重定向到 Leader。

值得注意的是 Learner 的特殊处理。Learner 是一种只接收日志但不参与投票的副本，通常用于异地容灾或读扩展。但在 Snapshot 恢复完成之前，Learner 的数据是不完整的——如果这时候允许读，客户端会看到不一致的结果。所以 `check_read_consistency()` 先检查 Learner 是否就绪，不就绪就返回 `CLUSTERDOWN Learner not ready for read`，而不是 MOVED。

**`write_through_raft()`** 是写命令的统一提交入口。它构建 `pb::RedisWriteRequest` protobuf（包含命令类型、KV 对、过期时间、SET 条件等），调用 `region->exec_redis_write(redis_req, &response)` 同步等待 Raft 共识和 apply 完成。如果返回 `NOT_LEADER`，则设置 MOVED 重定向；其他错误直接透传 `response.errmsg()`。成功时通过 `response.affected_rows()` 获取影响行数。

`exec_redis_write()` 内部的流程是标准的 Raft 提交：检查 Leader 身份 → 序列化请求到 `butil::IOBuf` → 创建 `DMLClosure` → 提交 `braft::Task` → 等待完成。这个方法阻塞当前 bthread 直到 Raft 日志被 committed 并 applied。apply 阶段调用的就是前面章节讨论过的 `region_redis.cpp` 中的 `apply_redis_write()`——在那里执行实际的 RocksDB 读写。

基类还提供了 **`read_key()`** 方法，用于直接从 RocksDB 读取 String 类型的值。它先尝试 Metadata CF（新编码），解码后检查类型和过期时间；如果 Metadata CF 没找到，回退到 Data CF（旧编码）。返回值约定：0 表示找到，1 表示不存在（或已过期），-1 表示内部错误。过期检查在读路径中完成——这是被动过期机制的一部分，我们在第 15 章详细讨论。

## 读命令与写命令的模式

理解了基类的三个核心方法，具体的 Handler 实现就变得非常模板化。

**读命令的标准模式**以 `GetCommandHandler` 为例：参数校验（args.size() == 2） → `check_route()` → `check_read_consistency()` → `read_key()` → 设置响应（找到则 `SetString(value)`，不存在则 `SetNullString()`，错误则 `SetError()`）。整个 Handler 不到 30 行代码。

**写命令的标准模式**以 `SetCommandHandler` 为例：解析参数（key、value、EX/PX/NX/XX 选项） → `check_route()` → 构建 KV 对和过期时间 → `write_through_raft()` → 根据 affected_count 设置响应。SET 命令的特殊之处在于它需要解析 EX/PX/NX/XX/KEEPTTL 等可选参数，将相对时间转换为绝对毫秒时间戳，然后将 NX/XX 条件通过 `set_condition` 参数传给 `write_through_raft()`。

NX/XX 的条件检查发生在 apply 层而非 Handler 层——这是前面章节反复强调的设计决策。如果在 Handler 层检查，两个并发的 `SET key value NX` 可能都读到"key 不存在"，然后都提交成功。只有在 Raft apply 层（串行执行）做检查，才能保证 NX 语义的正确性。

**多 key 命令**如 `MgetCommandHandler` 稍有不同。MGET 路由成功后，需要对每个 key 分别调用 `read_key()`，将结果收集到一个数组中返回。路由阶段已经保证了所有 key 在同一个 slot（否则返回 CROSSSLOT 错误），所以不需要跨 Region 操作。

## 返回值传递的工程妥协

写命令通过 Raft apply 后需要返回结果。对于简单的计数（比如 SADD 返回实际添加的元素数），`pb::StoreRes` 的 `affected_rows` 字段足够了。但有些命令需要返回字符串——INCRBYFLOAT 返回新值，GETSET 返回旧值，SPOP 返回被弹出的元素。

NeoKV 没有为这些返回值定义新的 protobuf 字段，而是复用了 `errmsg` 字段。在 apply 层，`response->set_errmsg(result_string)` 将结果塞进 errmsg；在 Handler 层，通过 `response.errmsg()` 取出来作为结果返回给客户端。

这是一个典型的工程妥协。定义专用字段更"正确"，但需要修改 protobuf 定义、重新生成代码、更新所有序列化/反序列化路径——对于一个几十个命令都需要不同返回类型的系统来说，工作量不小。而 errmsg 复用虽然语义上不够优雅，但它**有效**：errmsg 本身就是一个 string 字段，在成功路径上没有"真正的"错误消息与之冲突，用来传递结果完全可行。代码中遍布这种用法——`HINCRBYFLOAT`、`SPOP`、`INCR`、`INCRBYFLOAT`、`APPEND`、`GETSET`、`GETDEL`、`GETEX` 都通过 errmsg 返回结果。

如果要正式化这个设计，可以在 `pb::StoreRes` 中增加一个 `bytes result_payload` 字段，但考虑到当前方案的简洁性和已经被验证的稳定性，这更多是一个"未来优化"。

## Standalone 模式

`neo_redis_standalone`（`src/redis/neo_redis_standalone.cpp`）是一个单进程 Redis 测试服务器，用于本地开发和集成测试。它的设计目标是：一条命令启动，不依赖 MetaServer，用标准 Redis 客户端即可连接。

启动流程很直观：初始化 RocksDB → 启动 Raft 服务 → 创建一个覆盖全部 16384 个 slot 的单 Region → 注册到 Store 单例 → 初始化 RedisRouter 并重建 SlotTable → 注册命令，启动 Redis 服务 → 等待 Leader 选举 → 输出就绪标记。

几个设计值得注意。**单副本 Raft**——Region 的 `replica_num` 设为 1，`peers` 只有自己。单副本 Raft 会自动选举自己为 Leader，不需要等待多数派。这意味着写操作仍然走 Raft 日志（保证 crash recovery），但没有网络开销。**无需路由逻辑**——单 Region 覆盖所有 slot，`start_key` 和 `end_key` 都为空，任何 key 都路由到这个 Region。**`NEOKV_READY` 标记**——进程在 Leader 选举成功后向 stdout 输出 `NEOKV_READY port=16379`，Go 测试框架（`tests/gocase/`）通过检测这个标记来判断服务器是否可以接收连接。这是一个简单但关键的同步机制——如果测试在服务器就绪前就发送命令，会得到连接拒绝。

Standalone 模式完美复用了 NeoKV 的全部 Redis 实现代码。它不是一个"简化版"——所有命令的 Handler、路由、Raft apply 逻辑都完整执行。唯一的区别是只有一个 Region、一个副本、没有 MetaServer。这意味着在 Standalone 模式下发现的 bug，在集群模式下同样存在（反过来不一定成立，因为集群模式有 Region Split、Leader 转移等额外复杂度）。

## 检验你的理解

- brpc 的 RedisService 自动处理 RESP 协议解析。如果 brpc 不支持某个 Redis 命令的特殊响应格式（比如 SUBSCRIBE 的 push 消息），NeoKV 能绕过去吗？
- Handler 对象在注册时创建，进程生命周期内不销毁。如果一个 Handler 不小心在成员变量中保存了请求状态，会发生什么？
- `write_through_raft()` 同步等待 Raft 共识。在 Leader 宕机的场景下，客户端最多等多久？这个超时由什么控制？
- 返回值通过 `errmsg` 字段传递。在什么情况下这个 hack 会产生歧义——apply 层的"真正的"错误消息是否可能与返回值冲突？
- Standalone 模式只有一个 Raft 副本。如果进程崩溃后重启，数据会丢失吗？（提示：考虑 RocksDB 的 WAL 和 Raft 日志的关系。）

---

> 下一章：[14-路由与集群](./14-路由与集群.md) — 深入 Slot 路由机制、SlotTable 的无锁读设计，以及 NeoKV 如何兼容 Redis Cluster 协议让标准客户端无缝接入。
