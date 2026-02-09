# NeoKV 设计文档

基于 Braft + Rocksdb 基础设施，实现支持 Redis 协议的强一致分布式 KV 存储。

## 1. 项目定位

NeoKV 的目标是实现一个 **基于 Braft 的分布式 KV，支持 Redis 协议**。

项目并非从零开始，而是基于 BaikalDB 这一成熟代码库。BaikalDB 原本实现了完整的 SQL 引擎，但 NeoKV 不需要 SQL 能力，只复用了其中的：

- **Store 节点**：数据存储进程，管理本地 RocksDB 和多个 Region
- **Multi-Raft**：基于 braft 的多 Raft 组实现，每个 Region 是一个独立的 Raft 组
- **MetaServer**：集群元数据管理，负责 Region 分配、调度、成员变更
- **RocksDB 存储引擎**：单机共享一个 RocksDB 实例，多 Region 通过 key 前缀隔离

在此基础上，NeoKV 新增了 Redis 协议层，使客户端可以通过标准 `redis-cli` 或 Redis SDK 直接访问。

---

## 2. 系统架构

### 2.1 进程模型

NeoKV 是一个两进程的分布式系统：

| 进程 | 入口 | 职责 |
|------|------|------|
| **neoMeta** | `src/meta_server/main.cpp` | 集群元数据管理：Region 分配/调度、Schema 管理、TSO 时间戳服务。自身通过 Raft 实现高可用 |
| **neoStore** | `src/store/main.cpp` | 数据服务节点：承载多个 Region（每个是一个 Raft 组），对外暴露两个端口 |

neoStore 的两个端口：
- **brpc 端口** (`--store_port`)：内部 Raft 通信和 Store 间 RPC
- **Redis 端口** (`--redis_port`，默认 16379)：运行 `brpc::RedisService`，对外提供 RESP 协议

### 2.2 请求链路

```
Redis Client (redis-cli / SDK)
    |  RESP 协议
    v
brpc::RedisService (Store 进程内, port 16379)
    |  命令解析由 brpc 框架完成
    v
CommandHandler (GET/SET/DEL/...)
    |
    +-- 读路径: RedisRouter::route() -> check_read_consistency() -> RocksDB::Get()
    |       |
    |       +-- Leader: 直接本地读
    |       +-- Follower (use_read_index=true): ReadIndex 强一致读
    |       +-- Follower (use_read_index=false): 返回 MOVED 重定向到 Leader
    |
    +-- 写路径: RedisRouter::route() -> write_through_raft()
            |
            v
        构建 pb::StoreReq (op_type = OP_REDIS_WRITE)
            |
            v
        braft::Node::apply(task)  -- Raft 共识
            |
            v
        Region::on_apply() -> do_apply() -> apply_redis_write()
            |
            v
        RocksDB::WriteBatch (编码 key/value，含 TTL)
```

---

## 3. 核心组件

### 3.1 Region（Raft 状态机）

每个 Region 是一个 `braft::StateMachine`，管理一段连续的 slot 范围。

**关键实现** (`include/store/region.h`, `src/store/region.cpp`)：
- Raft 组 ID：`"region_" + region_id`
- 写入通过 `exec_redis_write()` 提交 Raft 日志，同步等待共识完成
- `on_apply()` -> `do_apply()` 根据 `OP_REDIS_WRITE` 分发到 `apply_redis_write()`
- 快照：Region 粒度导出/加载 SST 文件，不做整库 checkpoint

**OpType 定义** (`proto/optype.proto`)：
```protobuf
OP_REDIS_WRITE = 90;  // Redis 写操作（SET/DEL/EXPIRE/MSET）
```

`is_dml_op_type()` 仅对 `OP_REDIS_WRITE` 返回 true，这是当前唯一的数据操作类型。

### 3.2 Redis 写路径

写操作（SET/DEL/MSET/EXPIRE）的完整流程：

1. CommandHandler 解析参数，调用 `RedisRouter::route()` 路由到目标 Region
2. `write_through_raft()` 构建 `pb::RedisWriteRequest`，调用 `Region::exec_redis_write()`
3. Region 检查 leader 身份，构建 `pb::StoreReq`，提交 `braft::Task`
4. Raft 共识完成后，`apply_redis_write()` (`src/redis/region_redis.cpp`) 执行实际写入：
   - SET/MSET：编码 key/value，`WriteBatch::Put()`，支持 NX/XX 条件
   - DEL：检查存在性，`WriteBatch::Delete()`
   - EXPIRE/EXPIREAT：读取现有 value，重新编码 TTL

**Protobuf 定义** (`proto/store.interface.proto`)：
```protobuf
enum RedisCmd {
    REDIS_SET = 0; REDIS_DEL = 1; REDIS_EXPIRE = 2;
    REDIS_EXPIREAT = 3; REDIS_MSET = 4;
};
enum RedisSetCondition {
    REDIS_SET_CONDITION_NONE = 0; REDIS_SET_CONDITION_NX = 1; REDIS_SET_CONDITION_XX = 2;
};
```

### 3.3 Redis 读路径

读操作（GET/MGET/TTL）不经过 Raft，直接读 RocksDB。一致性模型取决于节点角色：

| 场景 | 行为 |
|------|------|
| Leader | 直接本地读，强一致 |
| Follower/Learner + `use_read_index=true` | ReadIndex 协议：向 Leader 获取 read index，等待本地 apply 追上后读取 |
| Follower/Learner + `use_read_index=false` | 返回 `MOVED` 重定向到 Leader |

**ReadIndex 流程** (`Region::follower_read_wait()`)：
1. 批量收集读请求，调用 `ask_leader_read_index()`
2. 向 Leader RPC 获取当前 `data_index`（或 Leader 本地 lease 快路径）
3. 等待本地 `_done_applied_index >= read_idx`
4. 读取本地 RocksDB，检查 TTL

### 3.4 RedisRouter（路由层）

`RedisRouter` (`include/redis/redis_router.h`, `src/redis/redis_router.cpp`) 负责：

**Slot 计算**：标准 Redis Cluster CRC16 算法
```cpp
uint16_t redis_slot(const std::string& key) {
    const std::string tag = extract_hash_tag(key);  // 支持 {tag} 语法
    return redis_crc16(tag) & 0x3FFF;               // mod 16384
}
```

**SlotTable**：O(1) 的 slot 到 Region 映射
```cpp
struct SlotTable {
    std::array<Entry, 16384> slots;  // 每个 slot 直接映射到 Region
};
```

**CROSSSLOT 校验**：多 key 命令（MGET/MSET/DEL）要求所有 key 在同一 slot，否则返回 `CROSSSLOT` 错误。通过 `RedisKeyPattern` 表定义每个命令的 key 位置（first_key_index, last_key_index, key_step）。

**MOVED 重定向**：
- 写请求落在非 Leader：返回 `-MOVED <slot> <leader_redis_addr>`
- 无 Region 服务该 slot：返回 `-CLUSTERDOWN Hash slot not served`

---

## 4. RocksDB 存储编码

### 4.1 Key 格式

所有 Redis 数据存储在 RocksDB 的 `DATA_CF` 列族，key 格式：

```
[region_id : 8B][index_id : 8B][slot : 2B][user_key_len : 4B][user_key : var][type_tag : 1B]
|<--- 16 字节前缀 (prefix) --->|<------------- suffix（slot 空间）--------------------->|
```

| 字段 | 大小 | 编码 | 说明 |
|------|------|------|------|
| region_id | 8 字节 | mem-comparable i64：`big_endian(val XOR 0x8000000000000000)` | Region 标识 |
| index_id | 8 字节 | 同上 | 表/索引标识（= table_id） |
| slot | 2 字节 | big-endian uint16 | Redis slot (0-16383) |
| user_key_len | 4 字节 | big-endian uint32 | 用户 key 长度 |
| user_key | 变长 | 原始字节 | Redis key |
| type_tag | 1 字节 | ASCII 字符 | 数据类型标识 |

**类型标签** (`include/redis/redis_codec.h`)：
```cpp
enum RedisTypeTag : uint8_t {
    REDIS_STRING = 'S',  REDIS_HASH = 'H',  REDIS_LIST = 'L',
    REDIS_SET = 'E',     REDIS_ZSET = 'Z',
};
```
当前仅实现了 `REDIS_STRING`，其余类型已定义但未实现命令处理。

### 4.2 Value 格式

```
[expire_at_ms : 8B][payload : var]
```

- `expire_at_ms`：big-endian int64，毫秒级 Unix 时间戳。`0` 表示永不过期
- `payload`：原始字节，即 Redis value

### 4.3 Region 范围编码

Region 的 `start_key` / `end_key` 定义在 suffix 空间（16 字节前缀之后）：
- `start_key = encode_u16be(slot_start)`
- `end_key = encode_u16be(slot_end + 1)`（tail region 的 `end_key` 为空，表示无上界）

### 4.4 SplitCompactionFilter

`SplitCompactionFilter` (`include/engine/split_compaction_filter.h`) 在 compaction 时清理 split 后超出范围的数据：
- 提取 key 的前 8 字节解码 `region_id`
- 提取 suffix（16 字节之后），与注册的 `end_key` 比较
- 若 `suffix >= end_key`，丢弃该 key

DATA_CF 配置了 16 字节的 `FixedPrefixTransform`，支持前缀 bloom filter 和高效 prefix seek。

### 4.5 TTL 机制

- **惰性过期**：每次读取时检查 `expire_at_ms`，过期则返回不存在
- **主动清理**：`RedisTTLCleaner` 后台线程（默认 60 秒间隔）扫描 Leader Region 的所有 key，批量删除过期数据（每 Region 每轮最多 1000 个）

---

## 5. 已实现的 Redis 命令

### 5.1 基础命令
| 命令 | Handler | 说明 |
|------|---------|------|
| PING | `PingCommandHandler` | 返回 PONG |
| ECHO | `EchoCommandHandler` | 回显参数 |

### 5.2 数据命令
| 命令 | Handler | 类型 | 说明 |
|------|---------|------|------|
| GET | `GetCommandHandler` | 读 | 直接 RocksDB 读取 |
| MGET | `MgetCommandHandler` | 读 | 多 key 读取（需同 slot） |
| TTL | `TtlCommandHandler` | 读 | 查询剩余过期时间 |
| SET | `SetCommandHandler` | 写(Raft) | 支持 EX/PX/NX/XX |
| DEL | `DelCommandHandler` | 写(Raft) | 支持多 key（需同 slot） |
| MSET | `MsetCommandHandler` | 写(Raft) | 多 key 写入（需同 slot） |
| EXPIRE | `ExpireCommandHandler` | 写(Raft) | 设置过期时间 |

### 5.3 Cluster 命令
| 命令 | 说明 |
|------|------|
| CLUSTER KEYSLOT | 计算 key 的 slot |
| CLUSTER INFO | 集群状态（slot 分配、节点数） |
| CLUSTER SLOTS | slot 范围与节点映射 |
| CLUSTER NODES | 节点列表（Redis Cluster 格式） |
| CLUSTER MYID | 当前节点 ID |

---

## 6. 集群管理

### 6.1 MetaServer 的角色

MetaServer 不包含 Redis 特定逻辑，它通用地管理 Region 生命周期：
- 创建 Region 时，`start_key`/`end_key` 编码 slot 范围
- 通过心跳下发 `add_peer`/`remove_peer`/`transfer_leader`/`split` 指令
- Store 处理心跳响应，执行 Region 变更

### 6.2 内部表

为满足 `SplitCompactionFilter` 和 `SchemaFactory` 的依赖，MetaServer 中创建了内部表：
- Database：`__redis__`
- Table：`kv`
- 提供 `table_id` / `index_id`，使 compaction filter 和 Region 初始化流程正常工作

### 6.3 Slot 路由表更新

`RedisRouter::rebuild_slot_table()` 在以下时机触发：
- Store 启动加载 Region 后
- Region 新增/删除/更新时（`Store::set_region()` / `Store::erase_region()`）
- 路由缓存未命中时（slow path 回退）

---

## 7. 配置项

| Flag | 默认值 | 说明 |
|------|--------|------|
| `redis_port` | 16379 | Redis 监听端口（0 = 禁用） |
| `redis_advertise_ip` | "" (自动检测) | MOVED/CLUSTER 响应中的 IP |
| `redis_advertise_port` | 0 (= redis_port) | MOVED/CLUSTER 响应中的端口 |
| `redis_cluster_enabled` | true | 是否启用 Cluster 语义 |
| `use_read_index` | false | 是否启用 Follower ReadIndex 强一致读 |
| `redis_ttl_cleanup_interval_s` | 60 | TTL 后台清理间隔（0 = 禁用） |

---

## 8. 项目结构

```
src/
├── common/          # 公共工具：日志、RPC 客户端、Schema 缓存、内存管理
├── engine/          # 存储引擎：RocksWrapper、QoS、自定义 RocksDB 配置
├── meta_server/     # MetaServer：集群管理、Region 调度、Schema 管理、TSO
├── raft/            # Raft 存储层：自定义 log/meta storage、快照文件系统适配
├── raft_meta/       # MetaServer 的 Raft 回调
├── raft_store/      # Store 的 Raft 回调
├── redis/           # Redis 协议层（NeoKV 新增）
│   ├── redis_service.cpp      # 命令处理器（GET/SET/DEL/...）
│   ├── redis_router.cpp       # Slot 路由（CRC16、CROSSSLOT、MOVED）
│   ├── redis_codec.cpp        # RocksDB key/value 编码
│   ├── region_redis.cpp       # Raft apply 逻辑
│   └── redis_ttl_cleaner.cpp  # TTL 后台清理
└── store/           # Store 节点：Region 管理、Raft 状态机、心跳
```

---

## 9. 后续规划

### 已完成
- Phase 0：服务端入口（PING/ECHO/CLUSTER）
- Phase 1：路由与强一致写（GET/SET/DEL/MGET/MSET/EXPIRE/TTL）
- Phase 2：Cluster 视图（CLUSTER SLOTS/NODES/INFO）+ ReadIndex Follower 读

### 待实现
- **数据类型扩展**：Hash/List/Set/ZSet（type_tag 和编码已预留）
- **更多 String 命令**：INCR/DECR/APPEND/STRLEN/SETNX/SETEX 等（路由 pattern 已注册）
- **Lua/EVAL**：脚本执行沙箱
- **PubSub**：发布订阅
  