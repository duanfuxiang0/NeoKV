# NeoKV Redis 协议实现设计文档

## 1. 概述

NeoKV 在分布式 KV 存储（Braft + RocksDB）之上实现 Redis 协议兼容层，使 Redis 客户端可以直接连接 NeoKV 进行读写操作。

与 Apache Kvrocks 的定位不同：
- **Kvrocks**：Redis 的 drop-in replacement，目标是 100% 协议兼容，异步主从复制
- **NeoKV**：分布式强一致 KV 存储，Redis 协议是接入层，写入通过 Raft 共识保证强一致

### 1.1 架构差异对比

| 维度 | Kvrocks | NeoKV |
|------|---------|-------|
| 写入路径 | 直接写 RocksDB | 必须走 Raft 共识 |
| Key 编码前缀 | `[ns_size][namespace][slot_id][user_key]` | `[region_id:8][index_id:8][slot:2]` |
| Column Family | 8 个 | 6 个已有 + 2 个新增 Redis 专用 |
| 元数据 | 独立 metadata CF，version-based lazy delete | 同方案（从 kvrocks 移植） |
| 复制 | 异步主从 | Raft 强一致 |
| 分片 | 16384 slot，手动迁移 | Region 自动分裂/合并 |

### 1.2 移植策略

从 kvrocks 移植数据结构编码和命令实现，适配 NeoKV 的：
- Region/Index 前缀编码
- Raft 写入路径（handler + apply 两层分离）
- RocksWrapper CF 管理

## 2. 存储层设计

### 2.1 Column Family 规划

| CF | 名称 | 用途 | 对应 kvrocks |
|----|------|------|-------------|
| `redis_metadata` | **新增** | 每个 Redis key 的元信息（type/expire/version/size） | metadata CF |
| `data` | **已有** | Hash field / Set member / List element / ZSet member 的子键数据 | default CF (PrimarySubkey) |
| `redis_zset_score` | **新增** | Sorted Set 的 score→member 二级索引 | zset_score CF (SecondarySubkey) |

### 2.2 Redis 类型枚举

```cpp
enum RedisType : uint8_t {
    kRedisNone   = 0,
    kRedisString = 1,
    kRedisHash   = 2,
    kRedisList   = 3,
    kRedisSet    = 4,
    kRedisZSet   = 5,
};
```

### 2.3 Metadata CF 编码

#### 2.3.1 Metadata Key 格式

```
[region_id:8][index_id:8][slot:2][user_key_len:4][user_key]
```

- `region_id` / `index_id`：MutTableKey.append_i64() 编码（mem-comparable，大端 + XOR 符号位）
- `slot`：大端 uint16，Redis hash slot (0-16383)
- `user_key_len`：大端 uint32
- `user_key`：原始字节

与当前 data CF 的 key 格式相比，**去掉了末尾的 type_tag 字节**，因为类型信息存储在 value 的 flags 中。

#### 2.3.2 Metadata Value 格式

**Flags 字节：**
```
[1-bit: 64bit标识（始终为1）] [3-bit: 保留] [4-bit: RedisType]
```

**String / JSON（单 KV 类型，IsSingleKVType = true）：**
```
[flags:1][expire_ms:8][inline_value...]
```

String 的值直接内联在 metadata value 中，不需要子键查找。GET/SET 只需一次 RocksDB 读写。

**Hash / List / Set / ZSet（复合类型）：**
```
[flags:1][expire_ms:8][version:8][size:8]
```

- `expire_ms`：绝对过期时间戳（毫秒），0 表示永不过期
- `version`：64-bit 版本号，用于 lazy deletion（见 §2.6）
- `size`：元素数量（hash field 数、list 长度、set 成员数等）

**List 类型额外字段：**
```
[flags:1][expire_ms:8][version:8][size:8][head:8][tail:8]
```

`head` / `tail` 是 uint64 指针，初始值为 `UINT64_MAX / 2`，支持 O(1) 两端 push/pop。

### 2.4 Subkey CF 编码（data CF）

用于复合类型的子元素存储。

#### 2.4.1 Subkey Key 格式

```
[region_id:8][index_id:8][slot:2][user_key_len:4][user_key][version:8][sub_key]
```

- 前缀与 metadata key 相同
- `version`：必须与 metadata 中的 version 匹配，这是 lazy deletion 的核心机制
- `sub_key`：类型相关的子键
  - Hash：field name
  - Set：member
  - List：fixed64(index)
  - ZSet：member

#### 2.4.2 Subkey Value 格式

- Hash：field value（原始字节）
- Set：空（member 就是 sub_key 本身）
- List：element value
- ZSet：encoded_double(score)

### 2.5 Score CF 编码（redis_zset_score CF）

仅用于 Sorted Set 的按分数范围查询。

#### 2.5.1 Score Key 格式

```
[region_id:8][index_id:8][slot:2][user_key_len:4][user_key][version:8][score:8][member]
```

- `score`：IEEE 754 double 编码为 8 字节，保证字节序与数值序一致
- `member`：成员名

#### 2.5.2 Score Value 格式

空（所有信息都在 key 中）。

### 2.6 Version-Based Lazy Deletion

这是从 kvrocks 移植的核心机制，解决大 key 删除的性能问题。

**问题**：删除一个有 100 万 field 的 hash，如果逐个删除子键，会阻塞 Raft apply 线程。

**方案**：
1. 删除操作只更新 metadata CF 中的 version（生成新 version）
2. 旧 version 的子键变成"孤儿"
3. RocksDB compaction filter 在后台异步清理孤儿子键

**Version 生成算法**（与 kvrocks 一致）：
```
version = (microsecond_timestamp << 11) | (counter % 2048)
```

53-bit 微秒时间戳 + 11-bit 原子计数器，保证单调递增且全局唯一。

**Compaction Filter 逻辑**：
- 对 data CF 中的每个子键，提取其 `(metadata_key, version)`
- 查询 metadata CF 获取当前 version
- 如果子键的 version != 当前 version，或 metadata 已过期/不存在，则删除该子键
- 设置 5 分钟宽限期，避免与正在进行的写入产生竞争

### 2.7 过期机制

- **被动过期**：每次读取 key 时检查 `expire_ms`，如果已过期则视为不存在
- **主动过期**：Compaction filter 在 compaction 时清理过期的 metadata 和子键
- **TTL Cleaner**：定时扫描触发 compaction（已有实现 `redis_ttl_cleaner.cpp`）

## 3. 写入路径设计

### 3.1 两层分离架构

NeoKV 的 Redis 写入必须经过 Raft 共识，因此每个写命令分为两层：

```
Redis Client
    │
    ▼
Command Handler (redis_service.cpp)
    │  解析参数、路由、构建 RedisWriteRequest
    ▼
Raft Consensus (braft)
    │  日志复制到多数节点
    ▼
Apply Logic (region_redis.cpp)
    │  在 Raft apply 线程中执行实际 RocksDB 写入
    ▼
RocksDB (metadata CF + data CF)
```

**Handler 层**（`redis_service.cpp`）：
- 解析 Redis 命令参数
- 通过 RedisRouter 路由到正确的 Region
- 构建 `RedisWriteRequest` protobuf
- 调用 `write_through_raft()` 提交到 Raft

**Apply 层**（`region_redis.cpp`）：
- 在 Raft apply 回调中执行
- 直接操作 RocksDB（metadata CF + data CF）
- 使用 WriteBatch 保证原子性
- 返回 affected_rows 等结果

### 3.2 读取路径

读命令不经过 Raft，直接读 RocksDB：
- Leader：直接读
- Follower：通过 ReadIndex 保证线性一致性
- 先读 metadata CF 获取类型/版本/过期信息
- 再根据类型读 data CF 的子键

## 4. 命令实现状态

### 4.1 已实现命令总览（截至 2026-02-09）

| 类型 | 命令 | 数量 |
|------|------|------|
| String | GET, SET, DEL, MGET, MSET, TTL, PTTL, EXISTS, STRLEN, TYPE, SETNX, SETEX, PSETEX, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, UNLINK, FLUSHDB, FLUSHALL, PING, ECHO, CLUSTER, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, GETRANGE, SETRANGE, GETSET, GETDEL, GETEX | 35 |
| Hash | HSET, HGET, HDEL, HMSET, HMGET, HGETALL, HKEYS, HVALS, HLEN, HEXISTS, HSETNX, HINCRBY, HINCRBYFLOAT, HRANDFIELD, HSCAN | 15 |
| Set | SADD, SREM, SISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SMISMEMBER, SSCAN, SINTER, SUNION, SDIFF, SINTERCARD, SMOVE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE | 17 |
| List | LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE, LPOS, LSET, LINSERT, LREM, LTRIM, LMOVE, LMPOP | 14 |
| ZSet | ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT, ZRANGE, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZINCRBY, ZRANGEBYLEX, ZLEXCOUNT, ZPOPMIN, ZPOPMAX, ZSCAN | 17 |
| Total |  | 98 |

### 4.2 最近新增实现（本轮）

**Set（10 个命令）**
- 读命令：SMISMEMBER, SSCAN, SINTER, SUNION, SDIFF, SINTERCARD
- 写命令：SMOVE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE

**Sorted Set（17 个命令完整集）**
- 写命令：ZADD, ZREM, ZINCRBY, ZPOPMIN, ZPOPMAX
- 读命令：ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT, ZRANGE, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZLEXCOUNT, ZSCAN
- 存储设计：`data` CF 存 member->score，`redis_zset_score` CF 存 score+member 二级索引

### 4.3 待实现（高优先级）

- `SCAN`, `KEYS`, `RANDOMKEY`, `OBJECT`, `DEBUG`
- `MULTI`/`EXEC`/`DISCARD`/`WATCH`（事务）
- `SUBSCRIBE`/`PUBLISH`（Pub/Sub）
- `EVAL`/`EVALSHA`（Lua）

### 4.4 String 类型迁移策略

当前 String 类型的数据存储在 data CF 中（Phase 1 实现）。引入 metadata CF 后需要迁移：

1. **新写入**：写入 metadata CF（inline value）
2. **读取**：先查 metadata CF；如果未找到，回退到旧 data CF 编码
3. **FlushDB**：同时清理两个 CF
4. **自然迁移**：旧 data CF 中的条目会随过期或覆盖写逐渐消失

### 4.5 测试状态（本地实测）

执行环境：`tests/gocase`，二进制 `output/bin/neo_redis_standalone`，Go 1.24。

- 目标回归（新增范围）通过：
  - `go test -count=1 -v ./unit/type/hash ./unit/type/set ./unit/type/zset -args -binPath=... -workspace=...`
  - 结果：`hash`、`set`、`zset` 全通过
- 全量 unit 回归：
  - `go test -count=1 ./unit/... -args -binPath=... -workspace=...`
  - 结果：全部通过（`expire/key/ping/hash/list/set/strings/zset`）

APPEND 修复与回归补充：
- 修复点：`REDIS_APPEND` apply 逻辑改为严格追加（不再从偏移 0 覆盖旧值）
- 新增回归用例：
  - `APPEND preserves existing TTL`
  - `APPEND empty string keeps original value`

## 5. 文件结构

```
include/redis/
├── redis_codec.h          # Key/Value 编码（当前 Phase 1 编码）
├── redis_metadata.h       # 新增：Metadata 类、InternalKey、RedisType 枚举
├── redis_common.h         # 公共常量和工具函数
├── redis_router.h         # Slot 路由和 CROSSSLOT 检查
├── redis_service.h        # 命令注册
└── redis_ttl_cleaner.h    # TTL 主动清理

src/redis/
├── redis_codec.cpp        # Key/Value 编码实现
├── redis_metadata.cpp     # 新增：Metadata encode/decode、version 生成
├── redis_router.cpp       # 路由实现
├── redis_service.cpp      # 命令 Handler 实现
├── redis_ttl_cleaner.cpp  # TTL 清理实现
├── region_redis.cpp       # Raft apply 逻辑
└── neo_redis_standalone.cpp # 单进程测试模式入口

tests/gocase/             # Go 集成测试（基于 kvrocks 测试框架）
├── util/                 # 测试工具（server 启动、TCP client、断言）
└── unit/
    ├── type/hash/        # Hash 命令测试
    ├── type/set/         # Set 命令测试
    ├── type/zset/        # ZSet 命令测试
    ├── type/list/        # List 命令测试
    ├── ping/             # PING 测试
    ├── type/strings/     # String 命令测试
    ├── expire/           # 过期相关测试
    └── key/              # EXISTS/TYPE/UNLINK 测试
```

## 6. Protobuf 定义

Redis 写命令通过 Raft 传播，使用以下 protobuf 消息（定义在 `proto/store.interface.proto`）：

```protobuf
enum RedisCmd {
    REDIS_SET       = 0;
    REDIS_DEL       = 1;
    REDIS_EXPIRE    = 2;
    REDIS_EXPIREAT  = 3;
    REDIS_MSET      = 4;
    REDIS_FLUSHDB   = 5;
    REDIS_PERSIST   = 6;
    REDIS_INCR           = 7;
    REDIS_INCRBYFLOAT_STR = 8;
    REDIS_APPEND         = 9;
    REDIS_HSET           = 10;
    REDIS_HDEL           = 11;
    REDIS_HSETNX         = 12;
    REDIS_HINCRBY        = 13;
    REDIS_HINCRBYFLOAT   = 14;
    REDIS_GETSET         = 15;
    REDIS_GETDEL         = 16;
    REDIS_GETEX          = 17;
    REDIS_SETRANGE       = 18;
    REDIS_SADD           = 20;
    REDIS_SREM           = 21;
    REDIS_SPOP           = 22;
    REDIS_SMOVE          = 23;
    REDIS_SINTERSTORE    = 24;
    REDIS_SUNIONSTORE    = 25;
    REDIS_SDIFFSTORE     = 26;
    REDIS_LPUSH          = 30;
    REDIS_RPUSH          = 31;
    REDIS_LPOP           = 32;
    REDIS_RPOP           = 33;
    REDIS_LSET           = 34;
    REDIS_LINSERT        = 35;
    REDIS_LREM           = 36;
    REDIS_LTRIM          = 37;
    REDIS_LMOVE          = 38;
    REDIS_LMPOP          = 39;
    REDIS_ZADD           = 40;
    REDIS_ZREM           = 41;
    REDIS_ZINCRBY        = 42;
    REDIS_ZPOPMIN        = 43;
    REDIS_ZPOPMAX        = 44;
}

message RedisWriteRequest {
    optional RedisCmd cmd = 1;
    repeated RedisKv kvs = 2;
    optional uint32 slot = 3;
    optional RedisSetCondition set_condition = 4;
    repeated RedisHashField fields = 5;
    repeated bytes members = 6;
    repeated bytes list_elements = 7;
    repeated RedisZSetEntry zset_entries = 8;
}
```

## 7. 构建和测试

```bash
# 构建（推荐）
mkdir -p build && cd build
cmake -DWITH_TESTS=ON ..
make -j$(nproc)

# 可选：强制重新生成 protobuf 后重编
rm -f proto/store.interface.pb.h proto/store.interface.pb.cc
make -j$(nproc)

# Go 集成测试（需 Go 1.24）
cd tests/gocase
mkdir -p workspace
go test -count=1 -v ./unit/type/hash ./unit/type/set ./unit/type/zset \
  -args \
  -binPath=/home/ubuntu/NeoKV/output/bin/neo_redis_standalone \
  -workspace=/home/ubuntu/NeoKV/tests/gocase/workspace

# 全量 unit 回归
go test -count=1 ./unit/... \
  -args \
  -binPath=/home/ubuntu/NeoKV/output/bin/neo_redis_standalone \
  -workspace=/home/ubuntu/NeoKV/tests/gocase/workspace
```

## 8. 参考

- [Apache Kvrocks](https://github.com/apache/kvrocks) - Redis on RocksDB 实现，Apache 2.0 协议
- [Redis 命令参考](https://redis.io/commands)
- NeoKV AGENTS.md - 项目构建和代码规范
