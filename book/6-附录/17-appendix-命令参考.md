# 附录：命令参考

NeoKV 目前实现了 **98 个** Redis 命令，覆盖 5 种数据类型和通用操作。这份参考按类别列出每个命令的语法、说明和复杂度。

几个阅读约定：`key` 是 Redis key，参与 CRC16 slot 路由；`[]` 表示可选参数；`...` 表示可重复。复杂度中的 N 通常指操作涉及的元素数量。所有写命令都经过 Raft 共识，实际延迟 = 命令本身复杂度 + Raft 日志复制延迟。

---

## 通用命令（3 个）

| 命令 | 语法 | 说明 | 复杂度 |
|------|------|------|--------|
| PING | `PING [message]` | 连通性测试，返回 PONG 或 echo message | O(1) |
| ECHO | `ECHO message` | 返回 message 本身 | O(1) |
| CLUSTER | `CLUSTER INFO\|NODES\|SLOTS\|MYID\|KEYSLOT key` | 集群信息查询，兼容 Redis Cluster 协议 | O(1) |

## Key 操作命令（13 个）

这些命令适用于所有数据类型，是 Redis 中最基础的 key 管理操作。

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
| FLUSHDB | `FLUSHDB` | 清空当前数据库（通过 version 递增实现，标记操作 O(1)） | O(1)* |
| FLUSHALL | `FLUSHALL` | FLUSHDB 的别名（NeoKV 只有单 DB） | O(1)* |

FLUSHDB 和 FLUSHALL 的标记操作是 O(1)，但后台 Compaction Filter 清理过期数据的代价与数据量成正比。两者共享同一个 `FlushdbCommandHandler`。UNLINK 和 DEL 也共享同一个 `DelCommandHandler`。

## String 命令（19 个）

String 是最简单的数据类型。在 NeoKV 中，String 的值直接内联在 REDIS_METADATA_CF 的 metadata 中，读写都不需要额外的 subkey 查找，效率等同于 RocksDB 的原生 Get/Put。

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

## Hash 命令（15 个）

Hash 的每个 field 在 DATA_CF 中作为独立的 KV 对存储，key 格式为 `[region][index][slot][user_key][version][field]`。Metadata CF 记录 field 数量（`size`），HLEN 因此是 O(1)。

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

## Set 命令（17 个）

Set 的成员存储在 DATA_CF 中，member 本身是 subkey 的一部分，value 为空字节串。成员存在性检查是一次 RocksDB point lookup。多 key 集合运算（SINTER/SUNION/SDIFF 及其 STORE 变体）要求所有 key 在同一个 slot 中，否则返回 CROSSSLOT 错误——使用 Hash Tag `{tag}` 可以确保相关 key 路由到同一 slot。

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

## List 命令（14 个）

List 使用 `head`/`tail` 双指针设计，每个元素以绝对索引作为 subkey 存储。这让 LINDEX 和 LSET 成为 O(1) 的 RocksDB point lookup（Redis 原版这两个操作是 O(N)）。代价是 LINSERT 和 LREM 需要重建索引，是 O(N) 操作。

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

\* LINDEX 和 LSET 在 NeoKV 中是 O(1)，因为每个元素以绝对索引作为 subkey 直接存储在 RocksDB 中，支持 point lookup。

## Sorted Set 命令（17 个）

ZSet 使用双 CF 索引：DATA_CF 存储 `member → score` 映射用于 point lookup，REDIS_ZSET_SCORE_CF 存储 `score+member` 用于按分数排序的范围查询。Score 使用 IEEE 754 位操作编码为字节序可比较的格式，负数翻转所有位，正数翻转符号位，保证 RocksDB 的字节序迭代等价于数值排序。

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

ZRANK 和 ZREVRANK 在 NeoKV 中是 O(N) 而非 Redis 的 O(log(N))。这是因为 RocksDB 没有跳表的 rank 概念，计算排名需要从头遍历 score 索引直到找到目标 member。

---

## 汇总

| 类别 | 数量 |
|------|------|
| 通用命令 | 3 |
| Key 操作 | 13 |
| String | 19 |
| Hash | 15 |
| Set | 17 |
| List | 14 |
| Sorted Set | 17 |
| **合计** | **98** |

## 未实现的常见命令

以下是一些 Redis 中常见但 NeoKV 尚未实现的命令，它们可以作为深入理解架构的练习方向。每个命令背后都藏着有趣的分布式问题。

**KEYS / SCAN**（★★★）——全局 key 扫描。难点在于数据分散在多个 Region 中，SCAN 需要跨 Region 聚合结果，同时维护 cursor 的连续性。如果扫描过程中发生 Region 分裂，cursor 的语义就变得模糊了。

**WATCH / MULTI / EXEC**（★★★★）——事务支持。单 Region 内的事务相对简单（在 Raft apply 层做检查），但跨 Region 的事务需要两阶段提交或类似的分布式事务协议，复杂度陡增。

**SUBSCRIBE / PUBLISH**（★★★）——Pub/Sub 消息。消息需要从发布者所在的节点路由到所有订阅者所在的节点，这是一个扇出（fan-out）问题。还需要考虑客户端断连后的消息丢失。

**BLPOP / BRPOP**（★★）——阻塞式列表操作。需要在服务端维护等待队列，当有新元素 push 进来时唤醒等待的客户端。在 Raft 架构下，等待和唤醒的时机需要仔细考虑。

**RENAME**（★★★）——跨 slot 的 key 重命名。如果源 key 和目标 key 在不同的 slot（不同的 Region），就变成了一个跨 Region 的原子操作。

**SORT**（★★）——通用排序，需要临时存储中间结果。

**DUMP / RESTORE**（★★）——key 的序列化和反序列化，用于跨实例迁移。

**MSETNX**（★）——原子批量 SETNX。路由表中已经注册了 key pattern，但 Handler 尚未实现，是一个很好的入门级练习。
