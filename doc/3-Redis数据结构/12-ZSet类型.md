# ZSet 类型

## 本章概览

在这一章中，我们将了解：

- ZSet 的双 CF 索引设计：为什么需要两个索引
- Score 的字节序保持编码：IEEE 754 double 如何变成可排序字节
- 写命令：ZADD/ZREM/ZINCRBY 如何同时维护两个索引
- 按 score 范围查询：ZRANGEBYSCORE/ZCOUNT 的实现
- 按 rank 查询：ZRANK/ZRANGE 的实现
- 按 lex 查询：ZRANGEBYLEX/ZLEXCOUNT 的实现

ZSet（Sorted Set）是五种数据类型中最复杂的——它需要同时支持按 member 查询和按 score 排序，因此使用了双 CF 索引。

**关键文件**：
- `src/redis/redis_service.cpp` — ZaddCommandHandler、ZrangebyscoreCommandHandler 等
- `src/redis/region_redis.cpp` — REDIS_ZADD、REDIS_ZREM、REDIS_ZINCRBY 等
- `include/redis/redis_metadata.h` — RedisZSetScoreKey

## 1. 双 CF 索引设计

ZSet 需要支持两种查询模式：

| 查询 | 示例 | 需要的索引 |
|------|------|-----------|
| 按 member 查 score | `ZSCORE key member` | member → score |
| 按 score 范围查 member | `ZRANGEBYSCORE key 0 100` | score → member |
| 按 rank 查 member | `ZRANGE key 0 -1` | score 有序遍历 |

单个 CF 无法同时满足这两种需求，因此 ZSet 使用两个索引：

```
REDIS_METADATA_CF (元信息):
  Key:   [prefix]["leaderboard"]
  Value: [flags=ZSet][expire_ms][version=100][size=3]

DATA_CF (member → score):
  Key:   [prefix]["leaderboard"][v100]["alice"]   → encoded_double(95.5)
  Key:   [prefix]["leaderboard"][v100]["bob"]     → encoded_double(87.0)
  Key:   [prefix]["leaderboard"][v100]["charlie"] → encoded_double(92.3)

REDIS_ZSET_SCORE_CF (score+member → 空):
  Key:   [prefix]["leaderboard"][v100][87.0_encoded]["bob"]      → ""
  Key:   [prefix]["leaderboard"][v100][92.3_encoded]["charlie"]  → ""
  Key:   [prefix]["leaderboard"][v100][95.5_encoded]["alice"]    → ""
```

- **DATA_CF**：按 member 排序，用于 `ZSCORE`、`ZRANK`、member 存在性检查
- **SCORE_CF**：按 score 排序，用于 `ZRANGEBYSCORE`、`ZRANGE`、`ZCOUNT`

两个索引必须始终保持一致——每次写入都需要同时更新两个 CF。

## 2. Score 编码

IEEE 754 double 的原始字节表示不能直接用于字节序比较。NeoKV 使用一种编码让 double 的字节序与数值序一致。

### 2.1 问题

IEEE 754 double 的二进制布局：

```
[符号位:1][指数:11][尾数:52]
```

直接按字节比较的问题：
- 负数的符号位为 1，正数为 0 → 负数"大于"正数 ❌
- 负数中，绝对值越大的数字二进制表示越大 → 排序反了 ❌

### 2.2 编码方案

```cpp
// src/redis/redis_metadata.cpp
void RedisZSetScoreKey::encode_score(double score, std::string* dst) {
    uint64_t bits;
    memcpy(&bits, &score, sizeof(bits));

    if (bits & (1ULL << 63)) {
        // 负数：全部取反
        bits = ~bits;
    } else {
        // 正数（含 +0）：翻转符号位
        bits ^= (1ULL << 63);
    }

    PutFixed64BigEndian(dst, bits);
}
```

**为什么负数全部取反，正数只翻转符号位？**

正数：翻转符号位后，0 变成 1，所有正数的最高位都是 1。正数之间的相对顺序不变（指数和尾数的排序本来就是正确的）。

负数：在 IEEE 754 中，负数的绝对值越大，二进制表示越大。但我们需要绝对值越大的负数排在越前面（-100 < -1）。全部取反后，原来大的变小，小的变大，顺序正好反转。同时最高位从 1 变成 0，保证所有负数排在正数前面。

编码后的排序：`-inf < -100 < -1 < -0 < +0 < 1 < 100 < +inf`

### 2.3 解码

```cpp
double RedisZSetScoreKey::decode_score(const rocksdb::Slice& data) {
    uint64_t bits = DecodeFixed64BigEndian(data.data());

    if (bits & (1ULL << 63)) {
        // 最高位为 1 → 原来是正数，翻转符号位还原
        bits ^= (1ULL << 63);
    } else {
        // 最高位为 0 → 原来是负数，全部取反还原
        bits = ~bits;
    }

    double score;
    memcpy(&score, &bits, sizeof(score));
    return score;
}
```

## 3. 写命令

### 3.1 ZADD

ZADD 是 ZSet 最核心的写命令，支持多种选项：

```
ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
```

```cpp
case pb::REDIS_ZADD: {
    RedisMetadata metadata;
    bool key_exists = read_metadata(key, &metadata);

    if (!key_exists || metadata.is_expired() || metadata.type != kRedisZSet) {
        metadata = RedisMetadata(kRedisZSet, true);
        metadata.size = 0;
    }

    int added = 0, updated = 0;

    for (auto& entry : request.zset_entries()) {
        double new_score = entry.score();
        std::string member = entry.member();

        // 1. 检查 member 是否已存在（查 DATA_CF）
        auto member_key = RedisSubkeyKey(..., metadata.version, member).encode();
        std::string old_score_bytes;
        bool member_exists = db->Get(read_options, data_cf,
                                      member_key, &old_score_bytes).ok();

        if (member_exists) {
            double old_score = decode_double(old_score_bytes);

            // NX: 只添加新 member，跳过已存在的
            if (condition == NX) continue;
            // GT: 只在新 score 大于旧 score 时更新
            if (condition == GT && new_score <= old_score) continue;
            // LT: 只在新 score 小于旧 score 时更新
            if (condition == LT && new_score >= old_score) continue;

            if (old_score == new_score) continue;  // score 没变，跳过

            // 删除旧的 Score CF 条目
            auto old_score_key = RedisZSetScoreKey(
                ..., metadata.version, old_score, member).encode();
            batch.Delete(score_cf, old_score_key);
            updated++;
        } else {
            // NX 以外的条件下，XX 要求 member 必须存在
            if (condition == XX) continue;
            metadata.size++;
            added++;
        }

        // 2. 写入 DATA_CF (member → score)
        batch.Put(data_cf, member_key, encode_double(new_score));

        // 3. 写入 SCORE_CF (score+member → 空)
        auto score_key = RedisZSetScoreKey(
            ..., metadata.version, new_score, member).encode();
        batch.Put(score_cf, score_key, "");
    }

    // 4. 更新 Metadata
    batch.Put(metadata_cf, meta_key, metadata.encode());
    db->Write(write_options, &batch);

    // CH 选项：返回 added + updated；否则只返回 added
    response->set_affected_rows(ch_flag ? added + updated : added);
    break;
}
```

关键点：更新 member 的 score 时，必须**先删除旧的 Score CF 条目，再写入新的**。否则 Score CF 中会残留旧的 score→member 映射。

### 3.2 ZREM

```cpp
case pb::REDIS_ZREM: {
    int removed = 0;
    for (auto& member : request.members()) {
        auto member_key = RedisSubkeyKey(..., metadata.version, member).encode();
        std::string score_bytes;

        if (db->Get(read_options, data_cf, member_key, &score_bytes).ok()) {
            double score = decode_double(score_bytes);

            // 同时删除两个索引
            batch.Delete(data_cf, member_key);
            batch.Delete(score_cf,
                RedisZSetScoreKey(..., metadata.version, score, member).encode());

            metadata.size--;
            removed++;
        }
    }
    // 更新 Metadata（size=0 时删除）
    // ...
}
```

### 3.3 ZINCRBY

```cpp
case pb::REDIS_ZINCRBY: {
    // 1. 读取现有 score
    double current_score = 0;
    bool member_exists = read_member_score(member, &current_score);

    // 2. 计算新 score
    double new_score = current_score + increment;

    // 3. 如果 member 已存在，删除旧的 Score CF 条目
    if (member_exists) {
        batch.Delete(score_cf,
            RedisZSetScoreKey(..., metadata.version, current_score, member).encode());
    } else {
        metadata.size++;
    }

    // 4. 写入新的两个索引
    batch.Put(data_cf, member_key, encode_double(new_score));
    batch.Put(score_cf,
        RedisZSetScoreKey(..., metadata.version, new_score, member).encode());

    // 返回新 score
    response->set_errmsg(format_double(new_score));
    break;
}
```

### 3.4 ZPOPMIN / ZPOPMAX

弹出 score 最小/最大的 member：

```cpp
case pb::REDIS_ZPOPMIN: {
    auto prefix = RedisZSetScoreKey(..., metadata.version, 0, "").encode_prefix();
    auto* iter = db->NewIterator(read_options, score_cf);

    // ZPOPMIN: 从前向后迭代（score 从小到大）
    // ZPOPMAX: 从后向前迭代（score 从大到小）
    iter->Seek(prefix);  // 或 SeekForPrev

    int popped = 0;
    while (iter->Valid() && popped < count) {
        // 解码 score 和 member
        auto [score, member] = decode_score_key(iter->key());

        // 删除两个索引
        batch.Delete(score_cf, iter->key());
        batch.Delete(data_cf,
            RedisSubkeyKey(..., metadata.version, member).encode());

        metadata.size--;
        popped++;
        iter->Next();
    }
    // ...
}
```

## 4. 读命令

### 4.1 ZSCORE

按 member 查 score，查 DATA_CF：

```cpp
// ZscoreCommandHandler::Run()
void Run() {
    auto member_key = RedisSubkeyKey(..., metadata.version, member).encode();
    std::string score_bytes;
    if (db->Get(read_options, data_cf, member_key, &score_bytes).ok()) {
        double score = decode_double(score_bytes);
        output->SetBulkString(format_double(score));
    } else {
        output->SetNullString();
    }
}
```

### 4.2 ZRANGEBYSCORE / ZREVRANGEBYSCORE

按 score 范围查询，迭代 SCORE_CF：

```cpp
// ZrangebyscoreCommandHandler::Run()
void Run() {
    // 构建 score 范围的起止 key
    auto start_key = RedisZSetScoreKey(
        ..., metadata.version, min_score, "").encode();
    auto end_key = RedisZSetScoreKey(
        ..., metadata.version, max_score, "\xff").encode();

    auto* iter = db->NewIterator(read_options, score_cf);
    std::vector<std::pair<std::string, double>> results;

    for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
        if (iter->key() > end_key) break;

        auto [score, member] = decode_score_key(iter->key());

        // 处理开区间/闭区间
        if (exclusive_min && score == min_score) continue;
        if (exclusive_max && score == max_score) break;

        results.push_back({member, score});

        // LIMIT offset count
        if (results.size() >= offset + count) break;
    }

    // 跳过 offset，返回 count 个结果
    output->SetArray(results, offset, count, with_scores);
}
```

ZREVRANGEBYSCORE 使用反向迭代（`SeekForPrev` + `Prev`）。

### 4.3 ZRANK / ZREVRANK

返回 member 的排名。需要在 SCORE_CF 中计数：

```cpp
// ZrankCommandHandler::Run()
void Run() {
    // 1. 先查 DATA_CF 获取 member 的 score
    double score = get_member_score(member);

    // 2. 在 SCORE_CF 中，从头迭代到该 member 的位置，计数
    auto prefix = RedisZSetScoreKey(..., metadata.version, 0, "").encode_prefix();
    auto target = RedisZSetScoreKey(
        ..., metadata.version, score, member).encode();

    auto* iter = db->NewIterator(read_options, score_cf);
    int64_t rank = 0;
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        if (iter->key() == target) {
            output->SetInteger(rank);
            return;
        }
        rank++;
    }
    output->SetNullString();  // member 不存在
}
```

ZRANK 的复杂度是 O(n)——需要从头计数到目标位置。这是 RocksDB 上实现排名查询的固有限制（没有跳表的 O(log n) 排名查询能力）。

### 4.4 ZRANGEBYLEX / ZLEXCOUNT

当所有 member 的 score 相同时，按 member 的字典序查询：

```cpp
// 在 SCORE_CF 中，相同 score 的 member 按字典序排列
// 因此可以用 score + member 前缀来做范围查询
```

### 4.5 ZSCAN

游标迭代，遍历 DATA_CF 中的 member→score 对。

## 5. Lazy Deletion 与双索引

当 DEL 一个 ZSet 时，Lazy Deletion 同时作用于两个 CF：

```
删除前：
  Metadata:  mykey → {version=100, size=1000000}
  DATA_CF:   mykey/v100/member1 → score1  (100 万条)
  SCORE_CF:  mykey/v100/score1/member1     (100 万条)

执行 DEL mykey：
  只删除 Metadata，DATA_CF 和 SCORE_CF 中的 200 万条记录不动

后续读取：
  查 Metadata → 不存在 → 返回 nil（不会查两个 CF）

后续写入 ZADD mykey 1.0 newmember：
  创建新 Metadata: {version=200, size=1}
  写入 DATA_CF:  mykey/v200/newmember → 1.0
  写入 SCORE_CF: mykey/v200/1.0/newmember
  旧的 v100 数据永远不会被读到
```

## 6. 已实现命令一览

| 命令 | 类型 | 说明 |
|------|------|------|
| ZADD | 写 | 添加/更新 member（支持 NX/XX/GT/LT/CH） |
| ZREM | 写 | 删除 member |
| ZSCORE | 读 | 查询 member 的 score |
| ZRANK / ZREVRANK | 读 | 查询 member 的排名 |
| ZCARD | 读 | 返回 member 数量（O(1)） |
| ZCOUNT | 读 | 统计 score 范围内的 member 数量 |
| ZRANGE | 读 | 按 rank 范围查询 |
| ZRANGEBYSCORE | 读 | 按 score 范围查询 |
| ZREVRANGE | 读 | 按 rank 反向查询 |
| ZREVRANGEBYSCORE | 读 | 按 score 反向查询 |
| ZINCRBY | 写 | score 增量 |
| ZRANGEBYLEX | 读 | 按字典序范围查询 |
| ZLEXCOUNT | 读 | 统计字典序范围内的数量 |
| ZPOPMIN / ZPOPMAX | 写 | 弹出 score 最小/最大的 member |
| ZSCAN | 读 | 游标迭代 |

## 检验你的理解

- ZSet 使用双 CF 索引，每次 ZADD 需要写入两个 CF。这对写入性能有什么影响？WriteBatch 能保证两个 CF 的原子性吗？
- Score 编码中，为什么负数需要"全部取反"而正数只需"翻转符号位"？如果对正数也全部取反会怎样？
- ZRANK 的复杂度是 O(n)。Redis 原生使用跳表实现 O(log n) 的 ZRANK。有没有办法在 RocksDB 上实现更高效的排名查询？
- 如果两个并发的 ZADD 更新同一个 member 的 score，会不会导致 Score CF 中残留旧条目？为什么不会？
- Lazy Deletion 时，ZSet 的孤儿数据量是其他类型的两倍（DATA_CF + SCORE_CF）。这对磁盘空间有什么影响？

---

> Part 3 到此结束。我们已经了解了五种 Redis 数据类型在 RocksDB 上的完整实现。
>
> 接下来进入 [Part 4: Redis 协议层](../part4-Redis协议层/13-RESP协议与命令框架.md)，我们将了解命令是如何从网络层到达这些实现的。
