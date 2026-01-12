# NeoKV 设计方案

在 NEOKV 代码库内，以“最小侵入 + 最大复用”的方式实现强一致 Redis Cluster

## 1. 目标与非目标

### 1.1 目标
- **协议兼容**：支持 Redis RESP 协议，兼容 `redis-cli`/常见 SDK
- **强一致**：这里 NEOKV 已经实现了 leader lease
- **Redis Cluster 语义一致**：
  - 多 key 命令要求 **同 slot**，否则返回 `CROSSSLOT`
  - 访问非 leader/非负责节点返回 `MOVED slot ip:port`
- **水平扩展**：采用 **slot-range Region（Multi-Raft）**，每个 Region 管理一个连续 slot 范围
- **最大化复用 NEOKV**：
  - MetaServer 的 Region 分配/调度/成员变更
  - Store 的 Region（braft StateMachine）、`MyRaftLogStorage/MyRaftMetaStorage`
  - `RocksWrapper`（单机 1 个 RocksDB 实例，多 Region 共享）
  - `SplitCompactionFilter` 用于 split 后清理旧范围数据

### 1.2 非目标（MVP 不做或延后）
- Redis 全量命令集一次性实现（先 String/Key/Cluster MVP）
- Lua/EVAL 的完整沙箱与 script cache（可按阶段推进）
- PubSub 的 shard channel 与迁移一致性（可后续）
- 与 Redis Sentinel 完全对齐（本方案走 Cluster）

---

## 2. 总体架构（进程内 RedisService）

### 2.1 进程/端口
在 **NEOKV Store 进程内**新增一个 Redis 端口，使用 brpc 的 RedisService：
- Store 已存在：store RPC、raft service、region 管理、RocksDB
- 新增：`brpc::RedisService` 监听 `--redis_port`

好处：
- 少一次 RPC hop（命令可直达本地 Region/raft）
- 可复用 Region 的 leader/follower read、snapshot/split 等机制

### 2.2 请求链路（读/写）

```
Client(redis-cli/SDK)
  │ RESP
  ▼
brpc::RedisService (Store进程内)
  │ 解析命令/参数
  ▼
RedisRouter
  │ 1) 提取 keys  2) 计算 slot  3) 同slot校验(CROSSSLOT)
  │ 4) slot-range -> region_id -> SmartRegion
  │ 5) leader 判断 -> MOVED 或继续
  ▼
Region (braft::StateMachine)
  │ 写：apply raft log（OP_REDIS_WRITE）
  │ 读：默认 leader lease（强一致）；
  ▼
RocksDB (RocksWrapper)
```

连接语义（必须明确）：
- 同一 TCP 连接的 pipeline 请求需要 **严格按请求顺序回包**；服务端需保证连接内 FIFO（例如连接维度串行执行/有序队列），跨连接可并发。

---

## 3. 复用 NEOKV 的关键点

### 3.1 MetaServer / Region 调度复用
复用 `meta_server/region_manager.*`、`store/store.cpp` 的心跳与 region 拉起流程：
- MetaServer 维护 **slot-range Region** 的 `pb::RegionInfo`
- Store 启动/心跳获取 region 列表与变更，创建/删除/更新本地 Region
- 成员变更、迁移、leader 变更复用既有机制（add_peer/remove_peer/transfer_leader/split/merge）

### 3.2 单机 RocksDB 共享复用
每个 Store 节点只有一个 RocksDB 实例（`RocksWrapper`），多个 Region 共享：
- raft log/meta：复用 `MyRaftLogStorage/MyRaftMetaStorage`（按 region_id 前缀隔离）
- redis 数据：复用 `data` CF（关键是 key 的前缀满足 region_id+index_id 假设）

### 3.3 SplitCompactionFilter 复用（决定 key 结构）
`SplitCompactionFilter` 在 compaction 末层会依据 `region_id + index_id` 前缀解析 key，并使用 `region_info.end_key` 比较来清理 split 后“超出新范围”的数据。
因此：Redis 的 RocksDB key 必须满足：
- 前 16 字节：必须与 NEOKV 的 `TableKey/MutTableKey` 编码一致（**不能写裸 int64**）：
  - 具体要求：使用 `MutTableKey.append_i64(region_id).append_i64(index_id)` 形成前缀（即 mem-comparable + big-endian + SIGN_MASK 翻转后的字节序列）
- `region_info.start_key/end_key` 定义在 **prefix 之后的 suffix 空间**，并且是 **exclusive upper bound**（`end_key` 为空表示无上界）

---

## 4. 元数据模型：slot-range Region

### 4.1 slot 与 Region 的关系
- Redis Cluster：slot = CRC16(tag(key)) % 16384（支持 `{tag}`）
- 本方案：一个 Region 管理一段连续 slot 范围（例如 0-4095）

### 4.2 RegionInfo 范围编码（start_key/end_key）
我们用 `pb::RegionInfo.start_key/end_key` 表达 suffix 空间的 range：
- **suffix 的排序规则**：以 `slot_id`（big-endian uint16）开头，保证同 slot 的 key 连续聚集（后续字段使用可解析的二进制安全编码，见 6.2）
- `start_key`：`encode_u16be(slot_start)`
- `end_key`（exclusive）：
  - 若 `slot_end == 16383`：`end_key = ""`（tail region，沿用 NEOKV “空 end_key 表示无上界” 的语义）
  - 否则：`end_key = encode_u16be(slot_end + 1)`

> 注：这样 `end_key` 的比较天然实现 “只保留 slot < slot_end+1 的所有 key”，无需拼接 `0xFF`。

### 4.3 peers 与对外 redis 地址
`pb::RegionInfo.peers` 里记录的是 Store 实例地址（ip:store_port / raft peer 相关），但 Redis 客户端需要 `ip:redis_port`。
约定：
- 同一个节点上：Store 与 Redis 使用同 IP，不同端口（默认）
- 支持对外通告地址（容器/NAT/多网卡）：新增配置
  - `--redis_advertise_ip` / `--redis_advertise_port`（不配则退化为 `leader_ip:FLAGS_redis_port`）
- Redis 重定向地址：`advertise_ip:advertise_port`

---

## 5. Router：CROSSSLOT / MOVED / Region 路由

### 5.1 slot 计算
直接复用 kvrocks 的实现逻辑（含 `{tag}`）：
- `slot_id = crc16(tag_or_key) & 0x3fff`

### 5.2 key 提取与同 slot 校验（CROSSSLOT）
对每个命令定义“哪些参数是 key”（类似 kvrocks 的 `CommandAttributes`）：
- 例如：
  - `GET key`：key=[1]
  - `SET key value ...`：key=[1]
  - `DEL key [key...]`：key=[1..]
  - `MGET key [key...]`：key=[1..]
  - `MSET key value [key value...]`：key=[1,3,5,...]
  - `EVAL script numkeys key...`：key=[3..3+numkeys-1]

路由规则：
- 若命令 key 数为 0：不做 slot 校验（如 `PING`、`INFO`、部分 `CLUSTER` 子命令）
- 若 key 数 >= 1：
  - 计算第一个 key 的 slot
  - 其它 key 若 slot 不一致：返回 `-CROSSSLOT ...`

> 这与 kvrocks 在 cluster 层统一做 slot 校验一致。

### 5.3 slot -> Region
Store 侧维护一个 slot-range 路由表（来自 MetaServer）：
- `slot_id -> region_id -> SmartRegion`
支持：
- region 新增/删除
- region 范围变更（split/merge）
- leader 变化

### 5.4 MOVED 规则（强一致默认）
当请求落在非 leader：
- 返回 `-MOVED <slot_id> <advertise_ip:advertise_port>`
当集群无对应 slot 服务：
- 返回 `-CLUSTERDOWN Hash slot not served`

---

## 6. RocksDB Key/Value 编码（复用 data CF）

### 6.1 固定表/索引（内部系统表）
为满足 `SplitCompactionFilter`/SchemaFactory 的索引信息依赖，需要在 MetaServer 中创建一个内部表：
- Database：`__redis__`（或 system namespace 下）
- Table：`kv`
- Primary Index：`PRIMARY`

该表只用于：
- 提供 `table_id/index_id`（让 SchemaFactory/compaction filter 能识别）
- Region 仍然带 `table_id`，Region init/heartbeat 流程不需要特殊逻辑

### 6.2 RocksDB Key 总体格式
存储在 `RocksWrapper::DATA_CF`，完整 key：

```
[region_id:8][primary_index_id:8][suffix...]
```

说明：这里的 `[region_id:8][primary_index_id:8]` 是“字节长度为 16 的前缀”，但它的**字节内容必须使用 NEOKV 的 i64 mem-comparable 编码**（见 3.3），避免 compaction filter / range 判断失效。

其中 suffix（Redis 逻辑 key 空间，必须二进制安全且可解析）：

```
[slot_id:2 u16be]
[user_key_len:4 u32be][user_key:bytes]     # Redis key 允许任意字节（含 '\0'）
[type_tag:1]                               # 'S'/'H'/'L'/'Z'/...
[subspace...]
```

说明：
- **slot_id 写入 key**：支持后续按 slot-range 高效 scan/导出/校验
- `type_tag/subkey`：为 hash/list/set/zset 等预留，MVP 可仅实现 String：
  - String：`type_tag = 'S'`，`subspace` 为空（即 key 唯一）
  - Hash：`type_tag = 'H'`，`subspace = [field_len:4 u32be][field:bytes]`
  - List：`type_tag = 'L'`，`subspace = [seq:8 u64be]`（元素内容放 value，或再追加 len+bytes）
  - ZSet：`type_tag = 'Z'`，建议做两类 key：
    - member 索引：`subspace = ['m'][member_len:4][member]`
    - score 索引：`subspace = ['s'][score:8 memcmp 编码][member_len:4][member]`（便于按 score scan）

### 6.3 end_key 的含义（exclusive upper bound）
`RegionInfo.end_key` 存的是 suffix 的上界（不含 region_id/index_id）：
- `end_key = encode_u16be(slot_end+1)` 或 tail 为空
这样 SplitCompactionFilter 能用字节序比较清理 split 后多余数据。

### 6.4 TTL（建议实现）
MVP TTL 策略：
- value 头部带 `expire_at_ms`（0 表示不过期）
- 读路径检查过期：过期则返回不存在（并可异步清理）
- 后台清理：
  - 周期性 scan（按 slot-range），删除过期 key
  - 或后续扩展：在 compaction/filter 中附带 TTL 过滤（需要更深集成）

---

## 7. Raft 日志与状态机（强一致写）

### 7.1 新增写入日志类型（建议）
建议新增 `pb::OpType OP_REDIS_WRITE`（或等价命名）：
- Raft log payload：`RedisWriteRequest { version, cmd, args, options, ttl, ... }`
- `Region::on_apply` 内按 cmd 执行，保证线性一致

为什么不直接复用 `OP_KV_BATCH`：
- `OP_KV_BATCH` 更适合纯 put/delete
- Redis 很多命令是 **读改写**（INCR、HINCRBY、LPUSH、ZADD…），用“高层命令日志”更自然，且易保持 Redis 语义

### 7.2 写路径
1) Router 校验 slot/region/leader
2) 构造 `RedisWriteRequest`
3) `_node.apply(task)` 提交
4) `on_apply` 执行写入 RocksDB（建议用 Transaction/WriteBatch）

### 7.3 读路径
强一致读（默认）：
- **leader ReadIndex**（推荐默认打开）：向 leader 获取 read index，等待本地 `_done_applied_index >= read_idx` 后再读 RocksDB（检查 TTL）
- **follower read（可选）**：在 `--enable_follower_read=true` 时允许；流程同样为 ReadIndex（向 leader 获取 read index + 本地 wait apply），然后本地读

性能优化（可选，需满足前置条件）：
- 若 NEOKV 已实现并验证了 braft 的 leader lease/term 安全读，可在满足条件时对 leader 读走 lease 快路径；否则保持 ReadIndex 作为默认。

---

## 8. 快照与恢复（Region 粒度）

原则：
- **禁止整库 checkpoint**（单机 1 DB，多 region 共享；整库 checkpoint 会把所有 region 混在一个 snapshot）
- 采用 NEOKV 现有思路：**Region 粒度导出/ingest**（SST 或文件集合）

对 Redis region：
- snapshot_save：导出该 region 的数据范围（按 `[region_id,index_id]` 前缀，取 `[prefix+start_key, prefix+end_key)`；tail region 的 `end_key==""` 表示无上界）
- snapshot_load：ingest 恢复并更新 applied/data index

---

## 9. Cluster 命令（对外协议）

MVP 支持：
- `CLUSTER KEYSLOT <key>`
- `CLUSTER SLOTS`
- `CLUSTER NODES`
- `CLUSTER INFO`

输出数据来源：
- Store 本地缓存的 slot-range -> region/peer/leader 视图（来自 MetaServer 心跳 + Region 状态）

重定向：
- `MOVED` 目标是 `<advertise_ip:advertise_port>`

---

## 10. 运维与调度（基于 NEOKV Region 调度）

### 10.1 扩缩容
复用 meta 的调度：
- 添加新 store：meta 将部分 slot-range region 迁移/复制到新节点
- 迁移手段：优先使用 Region split/merge + add_peer/remove_peer + transfer_leader

### 10.2 热点治理
当某个 slot-range 热点：
- 对应 Region 可 split 成更小的 slot-range
- 通过 meta 下发 split，并更新路由视图

### 10.3 写入保护（可选）
迁移窗口可借鉴 kvrocks 的做法返回 `TRYAGAIN`：
- 当 slot-range 正在迁移且处于 write-forbidden 阶段，写命令返回 `TRYAGAIN`

兼容性补充（建议尽早支持）：
- 支持 `ASK` / `ASKING` 语义与 importing/migrating 状态（最小闭环），避免迁移窗口大量 SDK 不可用：
  - 源端：对迁移中的 slot 写返回 `-ASK <slot> <target>`
  - 目标端：收到 `ASKING` 后允许对该 slot 临时写入/读取
  - `CLUSTER SLOTS/NODES` 需要能反映过渡状态（最小可用即可）

---

## 11. 配置项（建议）

### 11.1 Store 侧
- `--redis_port`：RedisService 监听端口（所有节点一致）
- `--redis_advertise_ip`：对外通告 IP（可选）
- `--redis_advertise_port`：对外通告端口（可选）
- `--enable_follower_read`：是否允许 follower read（默认 false）
- `--redis_cluster_enabled`：是否开启 cluster 语义（默认 true）

### 11.2 Meta 侧
- 初始化 `__redis__.kv` 表与初始 slot-range regions（0..16383 分片）
- 初始 region 划分策略：
  - N 个 region 均匀覆盖 slot-range（连续区间）
  - 每个 region replica_num 与 peers 分配复用现有策略

---

## 12. 开发里程碑（可直接开工）

### Phase 0：服务端入口
- 在 `store/main.cpp` 中挂载 `brpc::RedisService`（新端口）
- 增加最小命令：`PING`、`ECHO`、`CLUSTER KEYSLOT`

### Phase 1：路由与强一致写（String/Key）
- Router：slot 计算、CROSSSLOT、MOVED
- 实现：`GET/SET/DEL/MGET/MSET/EXPIRE/TTL`
- 写路径走 `OP_REDIS_WRITE`，读默认 ReadIndex（强一致）

### Phase 2：cluster 视图与 follower read（可选）
- `CLUSTER SLOTS/NODES/INFO`
- follower read（ReadIndex）开关

### Phase 3：类型扩展
- Hash/List/Set/ZSet（按 type_tag/subkey 扩展编码）
- 多 key 限制保持 Redis Cluster 一致（同 slot）

---

## 13. 关键风险与对策

- **SchemaFactory/index_info 依赖**：必须创建 `__redis__.kv` 内部表，确保 compaction filter/region init 可识别 index_id。
- **对外通告地址**：MOVED/CLUSTER SLOTS/NODES 返回的地址必须使用 `--redis_advertise_ip/port`（或有一致的默认推导），避免容器/NAT/多网卡环境下客户端不可达。
- **TTL 清理成本**：MVP 可“读时检查 + 后台清理”；后续可引入更高效的过期索引或 compaction TTL filter。
- **命令 key 提取表**：必须覆盖所有已实现命令，否则容易出现“漏检 CROSSSLOT”导致语义偏差。


