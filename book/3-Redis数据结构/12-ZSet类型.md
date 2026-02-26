# ZSet 类型

## 概览

- ZSet 的双 CF 索引设计：为什么一个 CF 不够
- Score 的字节序保持编码：IEEE 754 double 如何变成可排序字节
- ZADD 的双写与旧 Score 清理
- 按 score 范围查询：ZRANGEBYSCORE 的前缀迭代
- 排名查询的固有限制：ZRANK 为什么是 O(n)
- Lazy Deletion 与双索引的交互

ZSet（Sorted Set）是五种数据类型中最复杂的。它同时需要按 member 查询和按 score 排序，单个 CF 的排序维度无法同时满足两种需求，于是 NeoKV 引入了双 CF 索引。这也意味着每次写入都需要同时维护两个索引，每次删除也要同时清理两边——复杂度翻倍，但换来了丰富的查询能力。

## 为什么需要两个索引

先想想 ZSet 需要支持的查询模式。`ZSCORE key member` 按 member 查 score——这需要 member 作为 key。`ZRANGEBYSCORE key 0 100` 按 score 范围查 member——这需要 score 作为排序维度。`ZRANGE key 0 -1` 按 rank 查 member——这需要按 score 有序遍历。

一个 CF 只能按一种方式排序。如果按 member 排序，ZSCORE 是 O(1)，但 ZRANGEBYSCORE 就需要全表扫描。如果按 score 排序，范围查询快了，但按 member 查 score 又需要扫描。两种模式无法兼顾。

NeoKV 的解决方案是双 CF 索引：

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

**DATA_CF** 按 member 排序，用于 `ZSCORE`、member 存在性检查。value 是 8 字节的编码后 score。**SCORE_CF** 按 score 排序（相同 score 的 member 按字典序排列），用于 `ZRANGEBYSCORE`、`ZRANGE`、`ZCOUNT`。value 为空——所有信息都编码在 key 中。

两个索引必须始终保持一致。每次写入都同时更新两个 CF，每次删除也同时清理两边。这是 ZSet 复杂度的根源，也是它能力的来源。

## Score 编码：让 double 可以按字节比较

SCORE_CF 的 key 中包含 score，而 RocksDB 的 key 是按字节序排列的。这意味着我们需要一种编码，让 double 的字节表示的排列顺序与数值大小一致。但 IEEE 754 double 的原始字节表示做不到这一点。

IEEE 754 double 的二进制布局是 `[符号位:1][指数:11][尾数:52]`。直接按字节比较有两个问题：负数的符号位为 1，正数为 0，所以按字节比较时所有负数都"大于"所有正数。负数中，绝对值越大的数字二进制表示越大（因为指数更大），但数值上 -100 < -1——排序方向反了。

NeoKV 的编码方案很精炼：

```cpp
void RedisZSetScoreKey::encode_double(double val, std::string* dst) {
    uint64_t bits;
    std::memcpy(&bits, &val, 8);
    if (bits & (1ULL << 63)) {
        bits = ~bits;           // 负数：全部取反
    } else {
        bits |= (1ULL << 63);  // 正数：设置符号位为 1
    }
    uint64_t be = KeyEncoder::host_to_be64(bits);
    dst->append(reinterpret_cast<const char*>(&be), 8);
}
```

正数（符号位为 0）只需要把符号位设为 1。这让所有正数的最高位都是 1，保证排在负数后面。正数之间的相对顺序不变——指数和尾数的字节比较本来就是正确的。

负数（符号位为 1）需要**全部取反**。为什么？因为在 IEEE 754 中，负数有一个反直觉的特性：`-1.0` 的位表示大于 `-0.5` 的位表示（因为 -1.0 的指数更大），但数值上 `-1.0 < -0.5`。全部取反把这个反序翻转过来，同时把最高位从 1 变成 0，让所有负数排在正数前面。

编码后的顺序：`-inf < -100 < -1 < -0 < +0 < 1 < 100 < +inf`，字节序与数值序完全一致。

解码是逆过程：

```cpp
double RedisZSetScoreKey::decode_double(const char* data) {
    uint64_t be;
    std::memcpy(&be, data, 8);
    uint64_t bits = KeyEncoder::be_to_host64(be);
    if (bits & (1ULL << 63)) {
        bits &= ~(1ULL << 63);  // 最高位为 1 → 原来是正数，清除符号位
    } else {
        bits = ~bits;           // 最高位为 0 → 原来是负数，全部取反还原
    }
    double val;
    std::memcpy(&val, &bits, 8);
    return val;
}
```

这个编码方案不是 NeoKV 发明的——它在各种需要对浮点数做字节序比较的场景中广泛使用，但能在 Redis 兼容存储中看到它的实际应用，仍然很有工程美感。

## ZADD：双写的精确维护

ZADD 是 ZSet 最核心的写命令，支持丰富的选项：`ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]`。它的 Apply 层实现展示了双索引维护的全部复杂性：

```cpp
case pb::REDIS_ZADD: {
    int added = 0, updated = 0;

    for (auto& entry : redis_req.zset_entries()) {
        double new_score = entry.score();
        std::string member = entry.member();

        // 1. 检查 member 是否已存在（查 DATA_CF）
        std::string data_subkey = RedisSubkeyKey::encode(..., version, member);
        std::string old_score_bytes;
        auto s = rocks->get(ro, rocks->get_data_handle(), data_subkey, &old_score_bytes);
        bool member_exists = s.ok() && old_score_bytes.size() >= 8;

        if (member_exists) {
            double old_score = RedisZSetScoreKey::decode_double(old_score_bytes.data());
            if (condition == NX) continue;                         // NX：只添加新 member
            if (condition == GT && new_score <= old_score) continue; // GT：新 score 必须更大
            if (condition == LT && new_score >= old_score) continue; // LT：新 score 必须更小

            // 删除旧的 Score CF 条目
            std::string old_score_key = RedisZSetScoreKey::encode(..., version, old_score, member);
            batch.Delete(rocks->get_redis_zset_score_handle(), old_score_key);
            updated++;
        } else {
            if (condition == XX) continue;  // XX：只更新已存在的 member
            metadata.size++;
            added++;
        }

        // 2. 写入 DATA_CF (member → encoded_score)
        std::string new_score_bytes;
        RedisZSetScoreKey::encode_double(new_score, &new_score_bytes);
        batch.Put(rocks->get_data_handle(), data_subkey, new_score_bytes);

        // 3. 写入 SCORE_CF (score+member → 空)
        std::string new_score_key = RedisZSetScoreKey::encode(..., version, new_score, member);
        batch.Put(rocks->get_redis_zset_score_handle(), new_score_key, rocksdb::Slice());
    }

    // CH 选项：返回 added + updated；否则只返回 added
    response->set_affected_rows(ch_flag ? added + updated : added);
    break;
}
```

这段代码中最关键的一步是：**更新 member 的 score 时，必须先删除旧的 Score CF 条目，再写入新的**。如果忘记删除旧条目，Score CF 中就会残留旧的 `(old_score, member)` 映射——下次 ZRANGEBYSCORE 时，同一个 member 可能出现在两个不同的 score 位置上。

还要注意 GT/LT 条件的处理。GT 表示"只在新 score 大于旧 score 时更新"——这在排行榜场景中很常用：`ZADD leaderboard GT 100 alice` 只有当 alice 的新分数高于当前分数时才更新。LT 是反向场景。这些条件检查同样在 Apply 层执行，保证并发安全。

## ZREM 和 ZINCRBY：双索引的同步删除和更新

**ZREM** 删除 member 时必须同时清理两个 CF：

```cpp
case pb::REDIS_ZREM: {
    for (auto& member : request.members()) {
        auto data_subkey = RedisSubkeyKey::encode(..., version, member);
        std::string score_bytes;
        if (rocks->get(ro, rocks->get_data_handle(), data_subkey, &score_bytes).ok()) {
            double score = RedisZSetScoreKey::decode_double(score_bytes.data());
            // 同时删除两个索引
            batch.Delete(rocks->get_data_handle(), data_subkey);
            batch.Delete(rocks->get_redis_zset_score_handle(),
                RedisZSetScoreKey::encode(..., version, score, member));
            metadata.size--;
        }
    }
    break;
}
```

先从 DATA_CF 读出 member 的 score，然后用这个 score 构建 Score CF 的 key 来删除。如果反过来——先删 Score CF——你就不知道该删哪个 score 的条目了（因为 score 信息存在 DATA_CF 的 value 中）。

**ZINCRBY** 是"读 score、加增量、写回"的模式。与 ZADD 更新 score 一样，必须先删除旧的 Score CF 条目，再写入新的。如果 member 不存在，从 score 0 开始计算，同时递增 size。

## 读命令：充分利用双索引

**ZSCORE** 是最简单的读操作——直接查 DATA_CF，用 member 作为 key，value 中解码出 score。一次 Get，O(1)。

**ZRANGEBYSCORE** 按 score 范围查询，这是 Score CF 大显身手的地方。实现上，构建 `[prefix][version]` 前缀，在 SCORE_CF 上做前缀迭代，每个 key 中解码出 score 和 member：

```cpp
std::string prefix = RedisZSetScoreKey::encode_prefix(region_id, index_id, slot, user_key, version);
rocksdb::ReadOptions ro;
ro.prefix_same_as_start = true;
std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(ro, rocks->get_redis_zset_score_handle()));

for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix)) break;
    double score = RedisZSetScoreKey::decode_double(iter->key().data() + prefix.size());
    std::string member(iter->key().data() + prefix.size() + 8, 
                       iter->key().size() - prefix.size() - 8);
    entries.emplace_back(score, std::move(member));
}
```

Score CF 中的 key 已经按 score 排序（thanks to 前面的编码方案），所以迭代的结果天然是有序的。ZREVRANGEBYSCORE 只需要将结果反转。然后在 Handler 层根据 min/max 范围过滤，应用 LIMIT offset count。

**ZRANGE** 按 rank 范围查询，本质上也是在 Score CF 上迭代——因为 Score CF 是按 score 排序的，第 0 名就是第一个条目，第 1 名就是第二个条目。跳过 offset 个条目，取 count 个。

**ZCOUNT** 统计 score 范围内的 member 数量，同样基于 Score CF 迭代计数。

## ZRANK：O(n) 的固有限制

ZRANK 返回 member 在 ZSet 中的排名（从 0 开始，按 score 升序）。Redis 原生用跳表（skip list）实现，可以 O(log n) 查找排名。但在 RocksDB 上，没有跳表的层级指针，只能从头开始计数：

```cpp
// ZRankCommandHandler
// 1. 先查 DATA_CF 确认 member 存在并获取 score
double target_score = RedisZSetScoreKey::decode_double(score_bytes.data());

// 2. 在 SCORE_CF 中从头迭代，计数直到找到目标
std::string prefix = RedisZSetScoreKey::encode_prefix(...);
int64_t rank = 0;
for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    std::string cur_member(iter->key().data() + prefix.size() + 8,
                           iter->key().size() - prefix.size() - 8);
    if (cur_member == member) {
        output->SetInteger(rank);
        return;
    }
    ++rank;
}
```

从 Score CF 的起始位置开始，逐条计数直到找到目标 member。时间复杂度是 O(rank)——排名越靠后，越慢。对于一个有 100 万 member 的 ZSet，查询排名 99 万的 member 需要迭代 99 万条记录。

这是 RocksDB（或者说所有基于 LSM-Tree 的存储）在排名查询上的固有限制。LSM-Tree 不维护 key 的位置信息——它只知道"这个 key 存不存在"和"比这个 key 大的下一个 key 是什么"，但不知道"这个 key 是第几个"。要知道排名，只能从头数。

**ZREVRANK** 需要反向计数。一种做法是计算总数再减去正向 rank，另一种是从后往前迭代。

## ZPOPMIN / ZPOPMAX

弹出 score 最小/最大的 member。实现上，先从 Score CF 加载所有条目（已按 score 排序），然后从头部（ZPOPMIN）或尾部（ZPOPMAX）取出指定数量的条目，逐一从两个 CF 中删除：

```cpp
case pb::REDIS_ZPOPMIN:
case pb::REDIS_ZPOPMAX: {
    bool pop_max = (cmd == pb::REDIS_ZPOPMAX);
    // 加载所有条目（Score CF 迭代）
    // ...
    for (size_t i = 0; i < pop_count; ++i) {
        size_t idx = pop_max ? (entries.size() - 1 - i) : i;
        // 从两个 CF 中删除
        batch.Delete(rocks->get_data_handle(), data_subkey);
        batch.Delete(rocks->get_redis_zset_score_handle(), score_key);
    }
    break;
}
```

ZPOPMIN 从 `entries[0]` 开始取（score 最小），ZPOPMAX 从 `entries[last]` 开始取（score 最大）。当前实现需要先加载所有条目——对于大 ZSet 来说开销不小。一个优化方向是只迭代需要弹出的数量就停下（对 ZPOPMIN 来说很自然，因为 Score CF 本身就是升序的），但 ZPOPMAX 需要从尾部开始，RocksDB 的 `SeekForPrev` 可以支持但实现稍复杂。

## Lazy Deletion 与双索引

当 DEL 一个 ZSet 时，Lazy Deletion 同时作用于两个 CF——DATA_CF 和 SCORE_CF 中的孤儿数据量是其他复合类型的两倍。

```
删除前：
  Metadata:  mykey → {version=100, size=1000000}
  DATA_CF:   mykey/v100/member1 → score1  (100 万条)
  SCORE_CF:  mykey/v100/score1/member1     (100 万条)

执行 DEL mykey：
  只删除 Metadata，DATA_CF 和 SCORE_CF 中的 200 万条记录不动

后续写入 ZADD mykey 1.0 newmember：
  创建新 Metadata: {version=200, size=1}
  写入 DATA_CF:  mykey/v200/newmember → 1.0
  写入 SCORE_CF: mykey/v200/1.0/newmember
  旧的 v100 数据永远不会被读到
```

DEL 仍然是 O(1)——只删一条 Metadata。200 万条孤儿记录在 compaction 时被逐步清理。磁盘空间的回收速度取决于 compaction 的频率和策略。在写入密集的场景下，compaction 频繁触发，孤儿清理比较及时。在写入稀疏的场景下，可能需要手动触发 compaction 来回收空间。

## ZRANGEBYLEX 与 ZLEXCOUNT

当所有 member 的 score 相同时，ZSet 退化为一个按字典序排列的有序集合。ZRANGEBYLEX 按 member 的字典序做范围查询，ZLEXCOUNT 统计字典序范围内的数量。

这些命令利用了 Score CF 的一个特性：相同 score 的 member 在 Score CF 中按字典序排列（因为 key 格式是 `[prefix][version][score][member]`，score 相同时按 member 的字节序排列）。所以字典序范围查询可以直接在 Score CF 上做前缀迭代。

## 检验你的理解

- ZSet 使用双 CF 索引，每次 ZADD 需要写两个 CF。这对写入性能有什么影响？WriteBatch 能保证两个 CF 的原子性吗？
- Score 编码中，为什么正数用 `bits |= (1ULL << 63)`（设置符号位）而负数用 `bits = ~bits`（全部取反）？如果对正数也全部取反会怎样？
- ZRANK 的复杂度是 O(n)。Redis 原生用跳表实现 O(log n) 的 ZRANK。有没有办法在 RocksDB 上实现更高效的排名查询？（提示：考虑维护一个辅助的 count 索引）
- 更新 member 的 score 时，必须先删除旧的 Score CF 条目。如果忘记删除会怎样？WriteBatch 能保证"删旧 + 写新"的原子性吗？
- Lazy Deletion 时，ZSet 的孤儿数据量是 Hash/Set 的两倍。这对 compaction 的压力有什么影响？有没有办法优化？

---

> Part 3 到此结束。我们已经了解了五种 Redis 数据类型在 RocksDB 上的完整实现——从最简单的 String（单 KV 内联）到最复杂的 ZSet（双 CF 索引 + Score 编码）。每种类型都在 KV 存储的约束下做出了自己的工程权衡。
>
> 接下来进入 [Part 4: Redis 协议层](../4-Redis协议层/13-RESP协议与命令框架.md)，我们将了解命令是如何从网络层到达这些实现的——RESP 协议解析、命令路由、以及集群模式下的 MOVED/ASK 重定向。
