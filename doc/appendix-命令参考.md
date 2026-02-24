# 附录：命令参考

NeoKV 目前实现了 **98 个** Redis 命令，覆盖 5 种数据类型和通用操作。本附录按类别列出所有已实现命令的语法、说明和复杂度。

> **约定**：
> - `key` 表示 Redis key，所有 key 都参与 CRC16 Slot 路由
> - `[]` 表示可选参数
> - `...` 表示可重复参数
> - 复杂度中的 N 通常指操作涉及的元素数量
> - 所有写命令都经过 Raft 共识，实际延迟 = 命令本身复杂度 + Raft 日志复制延迟

---

## 通用命令（3 个）

| 命令 | 语法 | 说明 | 复杂度 |
|------|------|------|--------|
| PING | `PING [message]` | 连通性测试，返回 PONG 或 echo message | O(1) |
| ECHO | `ECHO message` | 返回 message 本身 | O(1) |
| CLUSTER | `CLUSTER INFO\|NODES\|SLOTS\|MYID\|KEYSLOT key` | 集群信息查询，兼容 Redis Cluster 协议 | O(1) |

## Key 操作命令（13 个）

| 命令 | 语法 | 说明 | 复杂度 |
|------|------|------|--------|
| DEL | `DEL key [key ...]` | 删除一个或多个 key（version-based lazy deletion） | O(N) |
| UNLINK | `UNLINK key [key ...]` | DEL 的别名，行为完全相同 | O(N) |
| EXISTS | `EXISTS key [key ...]` | 检查 key 是否存在，返回存在的 key 数量 | O(N) |
| TYPE | `TYPE key` | 返回 key 的数据类型（string/hash/set/list/zset） | O(1) |
| EXPIRE | `EXPIRE key seconds` | 设置 key 的过期时间（秒） | O(1) |
| EXPIREAT | `EXPIREAT key timestamp` | 设置 key 的过期时间为 Unix 时间戳（秒） | O(1) |
| PEXPIRE | `PEXPIRE key milliseconds` | 设置 key 的过期时间（毫秒） | O(1) |
| PEXPIREAT | `PEXPIREAT key ms-timestamp` | 设置 key 的过期时间为 Unix 时间戳（毫秒） | O(1) |
| TTL | `TTL key` | 返回 key 的剩余生存时间（秒），-1 表示永不过期，-2 表示不存在 | O(1) |
| PTTL | `PTTL key` | 返回 key 的剩余生存时间（毫秒） | O(1) |
| PERSIST | `PERSIST key` | 移除 key 的过期时间，使其永不过期 | O(1) |
| FLUSHDB | `FLUSHDB` | 清空当前数据库（通过 version 递增实现，O(1) 标记） | O(1)* |
| FLUSHALL | `FLUSHALL` | FLUSHDB 的别名，行为相同（NeoKV 只有单 DB） | O(1)* |

> *FLUSHDB/FLUSHALL 的标记操作是 O(1)，但后台 Compaction Filter 清理过期数据的代价与数据量成正比。

## String 命令（20 个）

| 命令 | 语法 | 说明 | 复杂度 |
|------|------|------|--------|
| GET | `GET key` | 获取 key 的值 | O(1) |
| SET | `SET key value [EX s] [PX ms] [NX\|XX] [GET]` | 设置 key 的值，支持过期时间和条件写入 | O(1) |
| SETNX | `SETNX key value` | 仅当 key 不存在时设置值 | O(1) |
| SETEX | `SETEX key seconds value` | 设置值并指定过期时间（秒） | O(1) |
| PSETEX | `PSETEX key milliseconds value` | 设置值并指定过期时间（毫秒） | O(1) |
| MGET | `MGET key [key ...]` | 批量获取多个 key 的值 | O(N) |
| MSET | `MSET key value [key value ...]` | 批量设置多个 key-value 对 | O(N) |
| GETSET | `GETSET key value` | 设置新值并返回旧值（已废弃，建议用 SET ... GET） | O(1) |
| GETDEL | `GETDEL key` | 获取值并删除 key | O(1) |
| GETEX | `GETEX key [EX s\|PX ms\|EXAT ts\|PXAT ms-ts\|PERSIST]` | 获取值并修改过期时间 | O(1) |
| STRLEN | `STRLEN key` | 返回 key 对应值的字符串长度 | O(1) |
| APPEND | `APPEND key value` | 追加值到已有字符串末尾，返回新长度 | O(1) |
| GETRANGE | `GETRANGE key start end` | 返回字符串的子串 | O(N) |
| SETRANGE | `SETRANGE key offset value` | 覆盖字符串指定偏移位置的内容 | O(N) |
| INCR | `INCR key` | 将 key 的值加 1（值必须是整数） | O(1) |
| DECR | `DECR key` | 将 key 的值减 1 | O(1) |
| INCRBY | `INCRBY key increment` | 将 key 的值增加指定整数 | O(1) |
| DECRBY | `DECRBY key decrement` | 将 key 的值减少指定整数 | O(1) |
| INCRBYFLOAT | `INCRBYFLOAT key increment` | 将 key 的值增加指定浮点数 | O(1) |

> String 类型在 NeoKV 中采用内联编码：值直接存储在 REDIS_METADATA_CF 的 metadata 中，无需额外的 subkey 查找。

## Hash 命令（15 个）

| 命令 | 语法 | 说明 | 复杂度 |
|------|------|------|--------|
| HSET | `HSET key field value [field value ...]` | 设置一个或多个 field-value 对 | O(N) |
| HGET | `HGET key field` | 获取指定 field 的值 | O(1) |
| HDEL | `HDEL key field [field ...]` | 删除一个或多个 field | O(N) |
| HMSET | `HMSET key field value [field value ...]` | 批量设置 field-value（已废弃，建议用 HSET） | O(N) |
| HMGET | `HMGET key field [field ...]` | 批量获取多个 field 的值 | O(N) |
| HGETALL | `HGETALL key` | 返回所有 field-value 对 | O(N) |
| HKEYS | `HKEYS key` | 返回所有 field 名 | O(N) |
| HVALS | `HVALS key` | 返回所有 field 的值 | O(N) |
| HLEN | `HLEN key` | 返回 field 数量 | O(1) |
| HEXISTS | `HEXISTS key field` | 检查 field 是否存在 | O(1) |
| HSETNX | `HSETNX key field value` | 仅当 field 不存在时设置值 | O(1) |
| HINCRBY | `HINCRBY key field increment` | 将 field 的值增加指定整数 | O(1) |
| HINCRBYFLOAT | `HINCRBYFLOAT key field increment` | 将 field 的值增加指定浮点数 | O(1) |
| HRANDFIELD | `HRANDFIELD key [count [WITHVALUES]]` | 随机返回一个或多个 field | O(N) |
| HSCAN | `HSCAN key cursor [MATCH pattern] [COUNT count]` | 增量迭代 Hash 的 field | O(N) |

> Hash 的每个 field 存储为 REDIS_SUBKEY_CF 中的独立 KV 对，key 格式为 `[slot][key][version][field]`。

## Set 命令（17 个）

| 命令 | 语法 | 说明 | 复杂度 |
|------|------|------|--------|
| SADD | `SADD key member [member ...]` | 添加一个或多个成员 | O(N) |
| SREM | `SREM key member [member ...]` | 移除一个或多个成员 | O(N) |
| SMEMBERS | `SMEMBERS key` | 返回所有成员 | O(N) |
| SISMEMBER | `SISMEMBER key member` | 检查 member 是否是集合成员 | O(1) |
| SMISMEMBER | `SMISMEMBER key member [member ...]` | 批量检查多个 member 是否是集合成员 | O(N) |
| SCARD | `SCARD key` | 返回集合的成员数量 | O(1) |
| SRANDMEMBER | `SRANDMEMBER key [count]` | 随机返回一个或多个成员 | O(N) |
| SPOP | `SPOP key [count]` | 随机移除并返回一个或多个成员 | O(N) |
| SSCAN | `SSCAN key cursor [MATCH pattern] [COUNT count]` | 增量迭代集合成员 | O(N) |
| SINTER | `SINTER key [key ...]` | 返回所有给定集合的交集 | O(N*M) |
| SUNION | `SUNION key [key ...]` | 返回所有给定集合的并集 | O(N) |
| SDIFF | `SDIFF key [key ...]` | 返回第一个集合与其他集合的差集 | O(N) |
| SINTERCARD | `SINTERCARD numkeys key [key ...] [LIMIT limit]` | 返回交集的基数（不返回成员） | O(N*M) |
| SMOVE | `SMOVE source destination member` | 将 member 从 source 移动到 destination | O(1) |
| SINTERSTORE | `SINTERSTORE destination key [key ...]` | 计算交集并存储到 destination | O(N*M) |
| SUNIONSTORE | `SUNIONSTORE destination key [key ...]` | 计算并集并存储到 destination | O(N) |
| SDIFFSTORE | `SDIFFSTORE destination key [key ...]` | 计算差集并存储到 destination | O(N) |

> 多 key 集合运算（SINTER/SUNION/SDIFF 及其 STORE 变体）要求所有 key 在同一个 Slot 中，否则返回 CROSSSLOT 错误。使用 Hash Tag `{tag}` 可以确保相关 key 路由到同一 Slot。

## List 命令（14 个）

| 命令 | 语法 | 说明 | 复杂度 |
|------|------|------|--------|
| LPUSH | `LPUSH key element [element ...]` | 从左侧插入一个或多个元素 | O(N) |
| RPUSH | `RPUSH key element [element ...]` | 从右侧插入一个或多个元素 | O(N) |
| LPOP | `LPOP key [count]` | 从左侧弹出一个或多个元素 | O(N) |
| RPOP | `RPOP key [count]` | 从右侧弹出一个或多个元素 | O(N) |
| LLEN | `LLEN key` | 返回列表长度 | O(1) |
| LINDEX | `LINDEX key index` | 获取指定索引位置的元素 | O(1)* |
| LRANGE | `LRANGE key start stop` | 返回指定范围内的元素 | O(N) |
| LPOS | `LPOS key element [RANK rank] [COUNT count] [MAXLEN len]` | 查找元素在列表中的位置 | O(N) |
| LSET | `LSET key index element` | 设置指定索引位置的元素值 | O(1)* |
| LINSERT | `LINSERT key BEFORE\|AFTER pivot element` | 在 pivot 元素前/后插入新元素 | O(N) |
| LREM | `LREM key count element` | 移除指定数量的匹配元素 | O(N) |
| LTRIM | `LTRIM key start stop` | 修剪列表，只保留指定范围内的元素 | O(N) |
| LMOVE | `LMOVE source destination LEFT\|RIGHT LEFT\|RIGHT` | 原子地将元素从一个列表移动到另一个列表 | O(1) |
| LMPOP | `LMPOP numkeys key [key ...] LEFT\|RIGHT [COUNT count]` | 从多个列表中弹出元素 | O(N) |

> *LINDEX 和 LSET 在 NeoKV 中是 O(1)，因为 List 使用 `left_index`/`right_index` 双指针设计，每个元素以其绝对索引作为 subkey 存储在 RocksDB 中，支持直接定位。

## Sorted Set 命令（17 个）

| 命令 | 语法 | 说明 | 复杂度 |
|------|------|------|--------|
| ZADD | `ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member [...]` | 添加成员或更新分数 | O(N·log(M)) |
| ZREM | `ZREM key member [member ...]` | 移除一个或多个成员 | O(N·log(M)) |
| ZSCORE | `ZSCORE key member` | 返回成员的分数 | O(1) |
| ZRANK | `ZRANK key member` | 返回成员的排名（从小到大，0-based） | O(N) |
| ZREVRANK | `ZREVRANK key member` | 返回成员的逆序排名（从大到小） | O(N) |
| ZCARD | `ZCARD key` | 返回有序集合的成员数量 | O(1) |
| ZCOUNT | `ZCOUNT key min max` | 返回分数在 [min, max] 范围内的成员数量 | O(N) |
| ZRANGE | `ZRANGE key min max [BYSCORE\|BYLEX] [REV] [LIMIT off cnt] [WITHSCORES]` | 通用范围查询（Redis 6.2+ 语法） | O(N) |
| ZRANGEBYSCORE | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT off cnt]` | 按分数范围查询成员 | O(N) |
| ZREVRANGE | `ZREVRANGE key start stop [WITHSCORES]` | 按排名逆序返回成员 | O(N) |
| ZREVRANGEBYSCORE | `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT off cnt]` | 按分数逆序范围查询 | O(N) |
| ZINCRBY | `ZINCRBY key increment member` | 增加成员的分数 | O(log(N)) |
| ZRANGEBYLEX | `ZRANGEBYLEX key min max [LIMIT offset count]` | 按字典序范围查询（分数相同时） | O(N) |
| ZLEXCOUNT | `ZLEXCOUNT key min max` | 返回字典序范围内的成员数量 | O(N) |
| ZPOPMIN | `ZPOPMIN key [count]` | 弹出分数最小的成员 | O(N·log(M)) |
| ZPOPMAX | `ZPOPMAX key [count]` | 弹出分数最大的成员 | O(N·log(M)) |
| ZSCAN | `ZSCAN key cursor [MATCH pattern] [COUNT count]` | 增量迭代有序集合成员 | O(N) |

> ZSet 在 NeoKV 中使用双 Column Family 索引：REDIS_SUBKEY_CF 存储 `member → score` 映射，REDIS_SCORE_CF 存储 `score+member` 用于范围查询。Score 使用 IEEE 754 位操作编码为字节序可比较的格式。

---

## 命令统计

| 类别 | 数量 | 说明 |
|------|------|------|
| 通用命令 | 3 | PING、ECHO、CLUSTER |
| Key 操作 | 13 | DEL、TTL、EXPIRE 系列、FLUSH 系列等 |
| String | 20 | GET/SET 系列、数值操作、子串操作 |
| Hash | 15 | HSET/HGET 系列、HSCAN |
| Set | 17 | 成员操作、集合运算、SSCAN |
| List | 14 | 双端操作、索引操作、LMOVE/LMPOP |
| Sorted Set | 17 | 分数操作、范围查询、ZSCAN |
| **合计** | **98** | |

> **注意**：FLUSHDB 和 FLUSHALL 共享同一个 Handler（FlushdbCommandHandler），但注册为两个独立命令名。UNLINK 和 DEL 共享同一个 Handler（DelCommandHandler）。

## 未实现的常见命令

以下是一些 Redis 中常见但 NeoKV 尚未实现的命令，可作为学习项目的扩展方向：

| 命令 | 说明 | 扩展难度 |
|------|------|----------|
| KEYS / SCAN | 全局 key 扫描，需要跨 Region 聚合 | ★★★ |
| WATCH / MULTI / EXEC | 事务支持，需要分布式事务协议 | ★★★★ |
| SUBSCRIBE / PUBLISH | Pub/Sub 消息，需要跨节点消息路由 | ★★★ |
| BLPOP / BRPOP | 阻塞式列表操作，需要异步等待机制 | ★★ |
| OBJECT | key 内部信息查询 | ★ |
| RENAME | 跨 Slot 的 key 重命名 | ★★★ |
| SORT | 通用排序，需要临时存储 | ★★ |
| DUMP / RESTORE | 序列化/反序列化 | ★★ |
| MSETNX | 原子批量 SETNX（路由表已注册但未实现 Handler） | ★ |

> 这些命令的实现可以作为深入理解 NeoKV 架构的练习。例如，实现 SCAN 需要理解 Region 分裂后如何跨多个 Region 聚合扫描结果；实现 MULTI/EXEC 需要理解分布式事务的两阶段提交。
