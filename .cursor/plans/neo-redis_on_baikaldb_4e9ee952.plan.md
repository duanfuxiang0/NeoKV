---
name: Neo-Redis on BaikalDB
overview: 在现有 BaikalDB 能稳定编译/跑测的前提下，按 Phase 0→1→2→3 逐步把 `neo-redis-design.md` 的 Redis Cluster MVP 落到 Store 进程内（brpc RedisService），每个阶段都有明确可验证的编译/单测/手工验收点。
todos:
  - id: baseline-build
    content: 基线编译通过（含 WITH_TESTS=ON）并跑通至少 1 个现有 test_*.cpp 二进制
    status: in_progress
  - id: phase0-redis-service
    content: 在 store 进程新增 redis_port 与 RedisService，支持 PING/ECHO/CLUSTER KEYSLOT + 对应单测
    status: pending
  - id: phase1-router-codec
    content: 实现 Router（slot/key/CROSSSLOT/region 选择 bootstrap）与 RocksDB key/value 编码 + 单测
    status: pending
  - id: phase1-raft-op
    content: 新增 OP_REDIS_WRITE + StoreReq redis 写载荷 + Region::do_apply 处理写命令，实现 GET/SET/DEL/MGET/MSET/EXPIRE/TTL
    status: pending
  - id: phase2-cluster-view
    content: 实现 CLUSTER SLOTS/NODES/INFO（基于 slot-range region 聚合）与可选 follower read 开关
    status: pending
---

# Neo-Redis on BaikalDB（分阶段可验证实现计划）

## 目标边界（按你选择）

- **先走 Store 侧 bootstrap**：Phase 1 先不改 MetaServer 初始化逻辑；路由优先从 Store 已加载的 `__redis__.kv` 表对应 region 列表推导（单 region 覆盖全 slot 也能跑通），后续再接入“slot-range region”与 cluster 视图。
- **测试优先级**：以 `test/test_*.cpp` 的单元测试为主；每个阶段补少量 `redis-cli` 可复现实测命令作为验收。

## Step 0：先把现有 BaikalDB 编译通过（基线）

- **动作**：按仓库规范跑 `cmake . && make -j$(nproc)`；再跑 `cmake -DWITH_TESTS=ON . && make -j$(nproc)`。
- **验收**：
- `make` 完成且无编译错误。
- 至少跑通 1~2 个现有测试二进制（例如 `./output/bin/test_table_key`）。

## Phase 0：服务端入口 + 最小命令（PING/ECHO/CLUSTER KEYSLOT）

### 代码落点

- 在 Store 进程新增 Redis 端口（新 `--redis_port`），不影响现有 `--store_port`。
- **主入口改动**：[`/home/ubuntu/BaikalDB/src/store/main.cpp`](/home/ubuntu/BaikalDB/src/store/main.cpp)
- 现状：只启动一个 `brpc::Server server` 监听 `FLAGS_store_port` 并挂载 `Store/CompactionServer`。
- 计划：新增第二个 `brpc::Server redis_server`（或 brpc 多端口启动方式二选一，优先独立 server），挂载 `brpc::RedisService` 派生服务类。

### 需要新增/修改

- **gflags**：新增 `--redis_port`（以及后续会用到的 `--redis_cluster_enabled` 预留）。
- **RedisService 实现**：新增一个处理 brpc Redis 请求的服务类（按 brpc 的 RedisService API 实现回调），支持：
- `PING` → `+PONG`
- `ECHO <msg>` → bulk string
- `CLUSTER KEYSLOT <key>` → integer（实现 `{tag}` 规则 + CRC16/16384）

### 单测（可验证）

- 新增 `test/test_redis_slot.cpp`：
- CRC16(“123456789”)=0x31C3 → slot=12739
- `{tag}` 生效：`slot("{user1000}.x") == slot("user1000")`

### 手工验收

- 启动 store 后：`redis-cli -p <redis_port> PING / ECHO / CLUSTER KEYSLOT` 正常返回。

## Phase 1：路由 + 强一致写（String/Key MVP）

> 目标：在不做 Meta 初始化改动前提下先闭环 GET/SET/DEL/MGET/MSET/EXPIRE/TTL；路由能做 slot 校验、CROSSSLOT；Region 选择先 bootstrap（从已存在的 `__redis__.kv` 表对应 region 推导），并能对非 leader 返回 MOVED。

### 1.1 Router：slot / key 提取 / CROSSSLOT / region 选择

- 新增 `RedisRouter`：
- 命令→keys 提取（覆盖 GET/SET/DEL/MGET/MSET/EXPIRE/TTL）
- 同 slot 校验：不同 slot 返回 `-CROSSSLOT Keys in request don't hash to the same slot`
- **slot→Region**（bootstrap 版）：
- 从 `SchemaFactory` 找到 `__redis__.kv` 的 `table_id` 与 `primary_index_id`
- `Store::traverse_region_map()` 过滤 `region->get_table_id()==redis_table_id`
- 依据 `region_info.start_key/end_key` 解码 slot-range：
- 空 start/end 视为全覆盖（便于先跑通）
- 非空按 `u16be` + exclusive end 规则解码

### 1.2 RocksDB Key/Value 编码（String MVP）

- 实现 key 编码函数：
- prefix：`MutTableKey.append_i64(region_id).append_i64(primary_index_id)`（满足 SplitCompactionFilter 的 mem-comparable 约束）
- suffix：`[slot:u16be][user_key_len:u32be][user_key][type_tag='S']`
- value 编码：`[expire_at_ms:i64be][payload bytes]`；`expire_at_ms==0` 表示不过期。

### 1.3 写路径：新增 OP_REDIS_WRITE（Raft log）

- proto：
- 修改 [`/home/ubuntu/BaikalDB/proto/optype.proto`](/home/ubuntu/BaikalDB/proto/optype.proto) 增加 `OP_REDIS_WRITE`（store 范围内编号）。
- 修改 [`/home/ubuntu/BaikalDB/proto/store.interface.proto`](/home/ubuntu/BaikalDB/proto/store.interface.proto) 的 `StoreReq` 增加 Redis 写入载荷字段（新增 `RedisWriteRequest` proto，或在同文件/新 proto 中定义并引用）。
- Region apply：
- 在 [`/home/ubuntu/BaikalDB/src/store/region.cpp`](/home/ubuntu/BaikalDB/src/store/region.cpp) 的 `do_apply()` switch 增加 `case pb::OP_REDIS_WRITE`，在 apply 侧执行具体命令语义（保证线性一致）。

### 1.4 读路径：默认强一致（leader + lease/read-index gate）

- 读命令在路由到 region 后：
- 调用 `Region::get_read_index()`（本地直调，利用其 leader+lease 判断），失败则返回 MOVED。
- 成功后读 RocksDB，并做 TTL 读时检查。

### 1.5 命令实现清单（MVP）

- `GET/SET/DEL/MGET/MSET/EXPIRE/TTL`
- `DEL/MGET/MSET` 强制同 slot（符合 Redis Cluster 语义）
- `TTL` 返回：不存在=-2，无过期=-1，其它=剩余秒数

### 单测（可验证）

- `test/test_redis_router.cpp`：key 提取 + CROSSSLOT 判定 + MSET 参数合法性。
- `test/test_redis_codec.cpp`：key 编码/解码、value TTL 编码/解码、过期判断。

### 手工验收（redis-cli）

- `SET/GET/DEL/MSET/MGET/EXPIRE/TTL` 在单节点/单 region 场景闭环；对非 leader 节点请求返回 `-MOVED <slot> <leader_ip>:<redis_port>`。

## Phase 2（可选优先级）：cluster 视图 + follower read

- `CLUSTER SLOTS/NODES/INFO`：由 Store 本地缓存的 redis regions（按 slot-range 聚合）输出。
- follower read：增加 `--enable_follower_read`，复用 Region 现有 follower-read/read-index 队列机制（必要时给 Region 增加一个对外可用的“等待 read-index”封装接口）。
- 单测：聚合输出的 slot-range 覆盖与格式基本断言。

## Phase 3：类型扩展（后续）

- 按 `type_tag/subkey` 扩展 Hash/List/Set/ZSet 的 key 空间，保持多 key 命令同 slot 约束。

## 交付顺序与回归策略

- 每完成一个 Phase：
- 先 `make -j$(nproc)`
- 再 `WITH_TESTS=ON` 编译并跑新增的 `test_redis_*`
- 最后用 `redis-cli` 做 5~10 条命令的 smoke test