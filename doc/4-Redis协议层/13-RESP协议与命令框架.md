# RESP 协议与命令框架

## 本章概览

在这一章中，我们将了解：

- brpc 的 RedisService 框架：如何零成本接入 RESP 协议
- 命令注册机制：98 个命令如何组织和注册
- RedisDataCommandHandler 基类：读写命令的通用逻辑
- 写命令的完整链路：从 Handler 到 Raft 到 Apply
- 读命令的完整链路：路由、一致性检查、RocksDB 读取
- Standalone 模式：单进程测试服务器的实现

**关键文件**：
- `include/redis/redis_service.h` — 公共 API 声明
- `src/redis/redis_service.cpp` — 所有命令 Handler 实现（约 6500 行）
- `src/redis/neo_redis_standalone.cpp` — 单进程测试服务器

## 1. brpc RedisService

NeoKV 没有自己实现 RESP 协议解析——它直接使用 brpc 框架内置的 `brpc::RedisService`。

brpc 的 RedisService 提供了：
- **RESP 协议解析**：自动将 Redis 客户端发来的二进制数据解析为命令和参数
- **命令分发**：根据命令名称路由到对应的 Handler
- **响应编码**：自动将 Handler 的输出编码为 RESP 格式返回给客户端
- **连接管理**：多路复用、keep-alive 等

我们只需要实现每个命令的 Handler 逻辑：

```cpp
// brpc 提供的接口
class RedisCommandHandler {
public:
    // 处理一个 Redis 命令
    // args: 命令参数（args[0] 是命令名，args[1..] 是参数）
    // output: 响应输出
    virtual RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                           brpc::RedisReply* output,
                                           bool flush_batched) = 0;
};
```

### 1.1 服务启动

```cpp
// src/store/store.cpp — Redis 服务启动
void Store::start_redis_service() {
    brpc::ServerOptions options;
    brpc::Server server;

    // 创建 RedisService 实例
    brpc::RedisService* redis_service = new brpc::RedisService();

    // 注册所有命令 Handler
    register_basic_redis_commands(redis_service);

    // 将 RedisService 添加到 brpc Server
    server.AddService(redis_service, brpc::SERVER_OWNS_SERVICE);

    // 在 Redis 端口上启动
    server.Start(FLAGS_redis_port, &options);
}
```

## 2. 命令注册

### 2.1 注册机制

每个 Redis 命令对应一个 Handler 类，通过 `AddCommandHandler` 注册到 RedisService：

```cpp
// src/redis/redis_service.cpp
void register_basic_redis_commands(brpc::RedisService* service) {
    auto add_handler = [&](const std::string& name, brpc::RedisCommandHandler* handler) {
        // 存储 handler 指针，保证生命周期
        static std::vector<std::unique_ptr<brpc::RedisCommandHandler>> handlers;
        handlers.emplace_back(handler);
        service->AddCommandHandler(name, handler);
    };

    // Phase 0: 基础命令（始终可用）
    add_handler("ping",    new PingCommandHandler());
    add_handler("echo",    new EchoCommandHandler());
    add_handler("cluster", new ClusterCommandHandler());

    // Phase 1: 核心数据命令
    add_handler("get",     new GetCommandHandler());
    add_handler("set",     new SetCommandHandler());
    add_handler("del",     new DelCommandHandler());
    add_handler("mget",    new MgetCommandHandler());
    add_handler("mset",    new MsetCommandHandler());
    // ...

    // Phase 3: Hash 命令
    add_handler("hset",    new HsetCommandHandler());
    add_handler("hget",    new HgetCommandHandler());
    // ... (15 个 Hash 命令)

    // Phase 4: Set 命令 (17 个)
    // Phase 5: List 命令 (14 个)
    // Phase 6: ZSet 命令 (17 个)
}
```

### 2.2 Handler 的生命周期

Handler 对象在注册时创建，存储在 static vector 中，与进程同生命周期。这避免了每次请求创建/销毁 Handler 的开销。

## 3. RedisDataCommandHandler 基类

大多数数据命令共享一套通用逻辑：路由、一致性检查、读写分发。NeoKV 将这些逻辑抽取到 `RedisDataCommandHandler` 基类中：

```cpp
// src/redis/redis_service.cpp
class RedisDataCommandHandler : public brpc::RedisCommandHandler {
protected:
    // 路由：根据 key 找到对应的 Region
    RedisRouteResult check_route(const std::string& key);
    RedisRouteResult check_route(const std::vector<std::string>& keys);

    // 读一致性检查：Leader 直读 / Follower ReadIndex / MOVED
    int check_read_consistency(SmartRegion region, brpc::RedisReply* output);

    // 读取 String key（Metadata CF 优先，回退 Data CF）
    Status read_key(const std::string& key, std::string* value);

    // 读取任意类型的 Metadata
    bool read_metadata(const std::string& key, RedisMetadata* metadata);

    // 写入通过 Raft
    void write_through_raft(SmartRegion region,
                            const pb::RedisWriteRequest& request,
                            pb::StoreRes* response,
                            brpc::RedisReply* output);
};
```

### 3.1 check_route()

路由是每个命令的第一步——根据 key 的 slot 找到负责的 Region：

```cpp
RedisRouteResult RedisDataCommandHandler::check_route(const std::string& key) {
    // 1. 计算 slot
    uint16_t slot = redis_slot(key);

    // 2. 在 SlotTable 中查找 Region
    auto region = RedisRouter::get_instance()->find_region_by_slot(slot);

    if (!region) {
        // 没有 Region 服务这个 slot
        return {nullptr, CLUSTERDOWN};
    }

    return {region, OK};
}
```

多 key 命令（MGET/MSET/DEL 等）会额外检查所有 key 是否在同一个 slot：

```cpp
RedisRouteResult check_route(const std::vector<std::string>& keys) {
    uint16_t first_slot = redis_slot(keys[0]);
    for (size_t i = 1; i < keys.size(); i++) {
        if (redis_slot(keys[i]) != first_slot) {
            return {nullptr, CROSSSLOT};  // 跨 slot 错误
        }
    }
    return check_route(keys[0]);
}
```

### 3.2 check_read_consistency()

读命令在路由之后需要检查一致性：

```cpp
int RedisDataCommandHandler::check_read_consistency(
        SmartRegion region, brpc::RedisReply* output) {
    if (region->is_leader()) {
        return 0;  // Leader 直接读
    }

    if (FLAGS_use_read_index) {
        // Follower ReadIndex：向 Leader 确认 committed index
        int64_t read_index = 0;
        region->follower_read_wait(&read_index);
        return 0;  // 等待完成，可以安全读取
    }

    // 既不是 Leader，也没启用 ReadIndex → MOVED 重定向
    auto leader_addr = region->get_leader_redis_addr();
    output->FormatError("MOVED %d %s", slot, leader_addr.c_str());
    return -1;
}
```

### 3.3 write_through_raft()

写命令的通用提交逻辑：

```cpp
void RedisDataCommandHandler::write_through_raft(
        SmartRegion region,
        const pb::RedisWriteRequest& write_req,
        pb::StoreRes* response,
        brpc::RedisReply* output) {
    // 1. 构建 StoreReq
    pb::StoreReq store_req;
    store_req.set_op_type(pb::OP_REDIS_WRITE);
    store_req.mutable_redis_write_request()->CopyFrom(write_req);

    // 2. 通过 Raft 提交（同步等待）
    BthreadCond cond;
    cond.increase();
    region->exec_redis_write(store_req, response, cond);
    cond.wait();  // 等待 Raft 共识 + apply 完成

    // 3. 检查结果
    if (response->errcode() == pb::NOT_LEADER) {
        // 不是 Leader，返回 MOVED
        auto leader_addr = region->get_leader_redis_addr();
        output->FormatError("MOVED %d %s", slot, leader_addr.c_str());
    }
}
```

## 4. 命令实现模式

### 4.1 读命令模式

```cpp
class GetCommandHandler : public RedisDataCommandHandler {
    RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                   brpc::RedisReply* output, bool) override {
        // 1. 路由
        auto result = check_route(args[1].as_string());
        if (result.error) { output->FormatError(...); return ...; }

        // 2. 一致性检查
        if (check_read_consistency(result.region, output) < 0) return ...;

        // 3. 读取数据
        std::string value;
        auto s = read_key(args[1].as_string(), &value);

        // 4. 返回结果
        if (s.ok()) {
            output->SetBulkString(value);
        } else {
            output->SetNullString();
        }
        return ...;
    }
};
```

### 4.2 写命令模式

```cpp
class SetCommandHandler : public RedisDataCommandHandler {
    RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                   brpc::RedisReply* output, bool) override {
        // 1. 解析参数
        std::string key = args[1].as_string();
        std::string value = args[2].as_string();
        // 解析 EX/PX/NX/XX 等选项...

        // 2. 路由
        auto result = check_route(key);
        if (result.error) { output->FormatError(...); return ...; }

        // 3. 构建写请求
        pb::RedisWriteRequest write_req;
        write_req.set_cmd(pb::REDIS_SET);
        auto* kv = write_req.add_kvs();
        kv->set_key(key);
        kv->set_value(value);

        // 4. 通过 Raft 提交
        pb::StoreRes response;
        write_through_raft(result.region, write_req, &response, output);

        // 5. 根据 response 设置输出
        if (response.errcode() == pb::SUCCESS) {
            output->SetStatus("OK");
        }
        return ...;
    }
};
```

### 4.3 返回值传递的 hack

Raft apply 的结果通过 `pb::StoreRes` 返回。对于简单的计数结果，使用 `affected_rows` 字段。但对于需要返回字符串的命令（如 GETSET 返回旧值、INCR 返回新值），NeoKV 复用了 `errmsg` 字段：

```cpp
// Apply 层（region_redis.cpp）
response->set_errmsg(old_value);  // 将旧值放在 errmsg 中

// Handler 层（redis_service.cpp）
std::string result = response.errmsg();  // 从 errmsg 中取出
output->SetBulkString(result);
```

这是一个实用的 hack——避免了为每种返回值类型定义新的 protobuf 字段。

## 5. Standalone 模式

`neo_redis_standalone` 是一个单进程测试服务器，用于本地开发和集成测试：

```cpp
// src/redis/neo_redis_standalone.cpp — 简化逻辑
int main(int argc, char* argv[]) {
    // 1. 初始化 RocksDB
    RocksWrapper::get_instance()->init(FLAGS_data_dir);

    // 2. 创建单个 Region（覆盖全部 16384 个 slot）
    auto region = create_region(/*region_id=*/1, /*table_id=*/1,
                                 /*start_slot=*/0, /*end_slot=*/16383);

    // 3. 启动单副本 Raft（自动选举为 Leader）
    region->start_raft_with_single_peer();

    // 4. 注册 Redis 命令，启动服务
    brpc::RedisService redis_service;
    register_basic_redis_commands(&redis_service);
    server.AddService(&redis_service);
    server.Start(FLAGS_redis_port);

    // 5. 输出就绪标记（测试框架检测用）
    printf("NEOKV_READY port=%d\n", FLAGS_redis_port);

    // 6. 等待退出信号
    wait_for_signal();
}
```

关键设计：
- **无需 MetaServer**：单副本 Raft 自动选举为 Leader
- **单 Region 覆盖全部 slot**：不需要路由逻辑
- **`NEOKV_READY` 标记**：Go 测试框架通过检测 stdout 中的这个标记来判断服务器是否就绪
- **优雅退出**：处理 SIGTERM/SIGINT，使用 `std::_Exit(0)` 避免全局析构器的问题

## 6. 代码导读

| 文件 | 内容 |
|------|------|
| `include/redis/redis_service.h` | `redis_crc16()`、`redis_slot()`、`register_basic_redis_commands()` |
| `src/redis/redis_service.cpp` | 所有命令 Handler（约 6500 行），搜索 `CommandHandler` 定位具体命令 |
| `src/redis/neo_redis_standalone.cpp` | 单进程测试服务器 |
| `proto/store.interface.proto` | `RedisCmd` 枚举、`RedisWriteRequest` 消息定义 |

## 检验你的理解

- brpc 的 RedisService 自动处理 RESP 协议解析。如果我们要支持 Redis 6.0 的 RESP3 协议，需要修改 NeoKV 的代码吗？
- NX/XX 条件检查在 apply 层执行而非 handler 层。如果在 handler 层检查，两个并发的 `SET key value NX` 会有什么问题？
- `write_through_raft()` 使用 `BthreadCond` 同步等待。如果 Raft 共识超时（比如 Leader 宕机），客户端会等多久？
- Standalone 模式下只有一个 Raft 副本。如果这个进程崩溃重启，数据会丢失吗？为什么？
- 返回值通过 `errmsg` 字段传递是一个 hack。如果要正式化这个设计，你会怎么修改 protobuf 定义？

---

> 下一章：[14-路由与集群](./14-路由与集群.md) — 我们将深入 Slot 路由机制和 Redis Cluster 协议兼容。
