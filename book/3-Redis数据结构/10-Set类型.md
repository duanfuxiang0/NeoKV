# Set 类型

## 概览

- Set 的存储模型：member 即 key、value 为空的巧妙设计
- SADD/SREM 的去重与 size 维护
- SPOP 的随机选择：Fisher-Yates 部分洗牌
- 集合运算：SINTER/SUNION/SDIFF 在 Handler 层的内存计算
- STORE 变体如何通过 Raft 写入结果

Set 的存储设计是五种类型中最简洁的——member 直接作为子键的 key，value 留空。这个"不存 value"的选择看似反直觉，实际上完美契合了集合语义。

## member 就是 key，value 为空

上一章我们看到 Hash 用 `[prefix][version][field] → field_value` 的结构存储子键。Set 的结构更简单——**member 就是 sub_key，value 为空**：

```
REDIS_METADATA_CF:
  Key:   [prefix]["myset"]
  Value: [flags=Set][expire_ms][version=100][size=3]

DATA_CF:
  Key:   [prefix]["myset"][version=100]["alice"]   → ""  (空)
  Key:   [prefix]["myset"][version=100]["bob"]     → ""  (空)
  Key:   [prefix]["myset"][version=100]["charlie"] → ""  (空)
```

value 为空——确切地说是 `rocksdb::Slice()`，零字节。这个设计有三个好处。**节省空间**：member 信息完全在 key 中，不需要在 value 里重复存储。**存在性检查即成员检查**：`SISMEMBER` 只需要一次 `rocks->get()` 判断 key 是否存在，status 返回 OK 就是成员，返回 NotFound 就不是，不需要读 value。**天然有序**：RocksDB 按 key 排序，member 自然按字典序排列，SMEMBERS 返回的结果是有序的（虽然 Redis 协议不保证这个顺序）。

## SADD：去重是核心逻辑

SADD 的实现模式与 HSET 类似，但多了一步**输入去重**：

```cpp
case pb::REDIS_SADD: {
    // 输入去重
    std::unordered_set<std::string> seen;
    
    int added = 0;
    for (auto& member : redis_req.members()) {
        if (!seen.insert(member).second) continue;  // 跳过重复输入
        
        auto subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, 
                                              user_key, metadata.version, member);
        std::string dummy;
        if (!rocks->get(read_options, rocks->get_data_handle(), subkey, &dummy).ok()) {
            batch.Put(rocks->get_data_handle(), subkey, rocksdb::Slice());  // 空 value
            metadata.size++;
            added++;
        }
    }
    batch.Put(rocks->get_redis_metadata_handle(), meta_key, metadata.encode());
    response->set_affected_rows(added);
    break;
}
```

为什么需要输入去重？想象 `SADD myset a b a c a`——如果不去重，代码会对 "a" 检查三次。第一次发现不存在，size 加一。第二次又发现不存在（因为 WriteBatch 还没提交到 RocksDB），size 又加一。最终 size 比实际多了 2。用 `std::unordered_set` 做输入去重，保证每个 member 只处理一次。

**SREM** 的模式与 HDEL 对称——检查存在性、删除子键、递减 size、size 为 0 时删除 Metadata。同样做了输入去重。

**SISMEMBER** 是最轻量的读操作之一：读 Metadata 拿 version，构建子键 key，一次 `rocks->get()` 检查存在性。SMISMEMBER 批量版本，对每个 member 做同样的检查，返回 0/1 数组。

**SCARD** 直接返回 `metadata.size`，O(1)。**SMEMBERS** 通过前缀迭代返回所有 member——和 HGETALL 的逻辑完全一样，只是只收集 key 中的 member 部分，不需要 value。

## SPOP：在 LSM-Tree 上做随机选择

SPOP 是 Set 中最有趣的命令——随机弹出一个或多个 member。在数组上做随机选择很简单（生成随机索引就行），但在 RocksDB 的有序 KV 存储上，事情就没那么直接了——LSM-Tree 不支持"给我第 N 个 key"这样的随机定位。

NeoKV 的做法是**先收集所有 member，再随机选择**：

```cpp
case pb::REDIS_SPOP: {
    // 1. 前缀迭代收集所有 member
    std::vector<std::string> all_members;
    std::vector<std::string> all_subkeys;
    // ... prefix iteration ...

    // 2. Fisher-Yates 部分洗牌
    std::mt19937 rng(std::random_device{}());
    size_t pop_count = std::min(static_cast<size_t>(count), all_members.size());
    for (size_t i = 0; i < pop_count; ++i) {
        std::uniform_int_distribution<size_t> dist(i, all_members.size() - 1);
        size_t j = dist(rng);
        if (i != j) {
            std::swap(all_members[i], all_members[j]);
            std::swap(all_subkeys[i], all_subkeys[j]);
        }
    }

    // 3. 删除选中的 member
    for (size_t i = 0; i < pop_count; i++) {
        batch.Delete(rocks->get_data_handle(), all_subkeys[i]);
        metadata.size--;
    }

    if (metadata.size <= 0) {
        batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
    } else {
        batch.Put(rocks->get_redis_metadata_handle(), meta_key, metadata.encode());
    }
    break;
}
```

这里用的是 **Fisher-Yates 部分洗牌**（也叫 Knuth shuffle 的前 k 步），而不是 `std::shuffle` 全洗牌。区别在于：如果你只需要随机选 3 个 member，不需要把整个数组都打乱——只做 3 步交换就够了，剩下的元素保持原样。时间复杂度从 O(n) 降到 O(k)（k 是弹出数量），虽然收集所有 member 本身就是 O(n)，但减少 swap 次数在实际中仍然有意义。

SPOP 在大 Set 上的性能不理想——一个有 100 万 member 的 Set，即使只 SPOP 一个，也需要先遍历 100 万条子键。这是 RocksDB 上实现随机访问的固有限制。Redis 原生基于内存的哈希表可以 O(1) 随机定位，NeoKV 做不到这一点。

**SRANDMEMBER** 与 SPOP 类似但不删除。支持负数 count（允许重复返回）。

## 集合运算：在内存中完成计算

SINTER/SUNION/SDIFF 是 Set 独有的命令，涉及多个 key 的读操作。它们在 Handler 层直接执行——因为是只读操作，不需要经过 Raft。

**SINTER**（交集）的实现策略是"逐步缩小"：先把第一个 Set 的所有 member 加载到 `std::unordered_set` 中作为候选集，然后对每个后续 Set，从候选集中移除不在该 Set 中的 member。每处理一个 Set，候选集就缩小一些。

```cpp
// 简化逻辑
std::unordered_set<std::string> result;
load_set_members_set(keys[0], &result);  // 加载第一个 Set

for (size_t i = 1; i < keys.size(); i++) {
    std::unordered_set<std::string> other;
    load_set_members_set(keys[i], &other);
    for (auto it = result.begin(); it != result.end(); ) {
        if (other.find(*it) == other.end()) {
            it = result.erase(it);  // 不在 other 中，移除
        } else {
            ++it;
        }
    }
}
```

**SUNION**（并集）更简单——把所有 Set 的 member 都塞进一个 `std::unordered_set`，去重是自动的。**SDIFF**（差集）从第一个 Set 开始，逐个移除后续 Set 中存在的 member。

**SINTERCARD** 是 Redis 7.0 新增的命令，只返回交集大小，支持 LIMIT 提前终止——当交集大小达到 limit 时立即返回，避免不必要的计算。

这些运算都在内存中完成。如果涉及的 Set 都很大（比如各有 100 万 member），内存开销会很显著。这也是为什么 Redis 建议对大 Set 的集合运算要谨慎——它可能导致临时的内存峰值。

## STORE 变体：计算在 Handler，写入通过 Raft

SINTERSTORE/SUNIONSTORE/SDIFFSTORE 将运算结果写入目标 key。这带来一个架构上的问题：集合运算需要读取多个 key（只读），但写入结果需要通过 Raft（写入）。NeoKV 的做法是把这两步分开——**Handler 层做计算，Apply 层做写入**。

Handler 层用上面相同的逻辑计算出结果集，然后把结果 member 列表作为 protobuf 的 `members` 字段，通过 Raft 提交。Apply 层收到后，直接写入目标 key：

```cpp
// Apply 层 — SINTERSTORE/SUNIONSTORE/SDIFFSTORE
RedisMetadata new_meta(kRedisSet, true);  // 新 version
new_meta.size = redis_req.members_size();
batch.Put(rocks->get_redis_metadata_handle(), dest_meta_key, new_meta.encode());

for (int i = 0; i < redis_req.members_size(); ++i) {
    std::string subkey = RedisSubkeyKey::encode(..., new_meta.version, redis_req.members(i));
    batch.Put(rocks->get_data_handle(), subkey, rocksdb::Slice());
}
```

如果目标 key 已存在，旧数据怎么处理？这里用了新的 version——旧子键的 version 不匹配，变成孤儿，通过 Lazy Deletion 机制在 compaction 时清理。不需要先遍历删除旧的 member，写入是 O(result_size)，不受旧数据量影响。

**SMOVE** 原子地将 member 从一个 Set 移动到另一个。实现上就是"从源 Set 删除 + 向目标 Set 添加"，全部在一个 WriteBatch 中，保证原子性。但注意：两个 key 必须在同一个 slot 中——跨 slot（意味着可能跨 Region）的原子操作目前不支持。

## 检验你的理解

- Set 的 value 为空，那 RocksDB 中这些空 value 会占用多少空间？（提示：RocksDB 的 SST 文件中，每条记录都有 key 长度、value 长度等元数据开销，即使 value 为空）
- SPOP 需要先收集所有 member 再随机选择。如果 Set 有 100 万个 member，有没有更高效的方案？（提示：考虑 RocksDB 的 `SeekToLast()` 和随机前缀探测）
- SINTERSTORE 如果目标 key 已存在且有大量 member，使用新 version 避免了逐条删除旧数据。这个策略有什么代价？
- 集合运算涉及多个 key。如果这些 key 不在同一个 slot，NeoKV 会返回 CROSSSLOT 错误。为什么跨 slot 的集合运算很难实现？
- SADD 中的输入去重用了 `std::unordered_set`。如果不做这步去重，在什么情况下会导致 size 计数错误？

---

> 下一章：[11-List 类型](./11-List类型.md) — 我们将了解 List 的双指针设计——head/tail 从 UINT64_MAX/2 开始的巧妙构思，以及如何在 KV 存储上实现 O(1) 的 push/pop。
