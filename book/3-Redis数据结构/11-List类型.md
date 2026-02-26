# List 类型

## 概览

- List 的存储模型：双指针（head/tail）实现 O(1) 两端操作
- LPUSH/RPUSH/LPOP/RPOP 的指针移动
- 随机访问：LINDEX 的 O(1) 实现——NeoKV 反超 Redis 原生的地方
- LINSERT/LREM 的代价：元素搬移与索引重建
- LMOVE 的跨 List 原子操作

List 是五种数据类型中存储设计最独特的。Hash 和 Set 用字符串作子键，List 用 uint64 索引。Hash 和 Set 的子键没有内在顺序要求，List 必须维护严格的元素顺序。这些差异催生了一个精巧的双指针设计。

## 从中间开始的双指针

List 的 Metadata 比其他复合类型多了两个字段——head 和 tail：

```
REDIS_METADATA_CF:
  Key:   [prefix]["mylist"]
  Value: [flags=List][expire_ms][version=100][size=3][head=H][tail=T]

DATA_CF:
  Key:   [prefix]["mylist"][version=100][H]     → "first"
  Key:   [prefix]["mylist"][version=100][H+1]   → "second"
  Key:   [prefix]["mylist"][version=100][H+2]   → "third"
                                         ↑ T = H+3（指向下一个空位）
```

head 和 tail 都是 uint64，初始值设定在一个看似奇怪的位置——`UINT64_MAX / 2`，也就是约 9.2 × 10^18：

```cpp
RedisListMetadata::RedisListMetadata(bool generate_version)
    : RedisMetadata(kRedisList, generate_version),
      head(UINT64_MAX / 2), tail(UINT64_MAX / 2) {}
```

为什么从中间开始？因为 List 需要支持两端操作——LPUSH 往左边加元素，RPUSH 往右边加元素。如果从 0 开始，LPUSH 就需要一个负数索引。但 RocksDB 的 key 是按无符号字节序排列的，负数索引会排在正数后面，破坏元素的逻辑顺序。从中间开始，两端都有充足的增长空间——即使一个 List 只做 LPUSH，理论上也能存 `UINT64_MAX / 2`（约 9.2 × 10^18）个元素，这在实际中等同于无限。

```
                    UINT64_MAX/2
                         │
    ◄── LPUSH 方向 ──── │ ──── RPUSH 方向 ──►
                         │
    [elem3][elem2][elem1]│[elem4][elem5][elem6]
     head-2 head-1 head  tail  tail+1 tail+2
```

子键中的 index 使用大端编码（`encode_list_index()`），保证 RocksDB 中的字节序与数值序一致。前缀迭代时，元素自然按索引顺序排列——从 head 到 tail-1，就是 List 中从左到右的顺序。

## Push 和 Pop：O(1) 的两端操作

**LPUSH** 先递减 head，再在 head 位置写入元素。**RPUSH** 先在 tail 位置写入元素，再递增 tail。两个方向的逻辑是对称的：

```cpp
case pb::REDIS_LPUSH:
case pb::REDIS_RPUSH: {
    bool is_lpush = (cmd == pb::REDIS_LPUSH);

    for (int i = 0; i < count; ++i) {
        uint64_t idx;
        if (is_lpush) {
            --list_meta.head;
            idx = list_meta.head;
        } else {
            idx = list_meta.tail;
            ++list_meta.tail;
        }
        std::string index_key = encode_list_index(idx);
        std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot,
                                                     user_key, list_meta.version, index_key);
        batch.Put(rocks->get_data_handle(), subkey, element);
    }
    list_meta.size += count;
    // 更新 Metadata...
    break;
}
```

注意 LPUSH 多个元素时的顺序：`LPUSH mylist a b c` 的结果是 `[c, b, a, ...]`。因为 a 先被推入（head 从 H 变为 H-1），然后 b 推入（head 变为 H-2），最后 c 推入（head 变为 H-3）。最后推入的 c 在最左边。这与 Redis 的行为一致。

**LPOP/RPOP** 是对称的弹出操作：

```cpp
case pb::REDIS_LPOP:
case pb::REDIS_RPOP: {
    bool is_lpop = (cmd == pb::REDIS_LPOP);
    uint64_t pop_count = std::min(static_cast<uint64_t>(count), list_meta.size);

    for (uint64_t i = 0; i < pop_count; ++i) {
        uint64_t idx;
        if (is_lpop) {
            idx = list_meta.head;
            ++list_meta.head;
        } else {
            --list_meta.tail;
            idx = list_meta.tail;
        }
        std::string subkey = RedisSubkeyKey::encode(..., encode_list_index(idx));
        // 读取并删除
        batch.Delete(rocks->get_data_handle(), subkey);
    }
    list_meta.size -= pop_count;
    // size 为 0 时删除 Metadata
    break;
}
```

每次 push/pop 只需要一次 RocksDB Put/Delete 加一次 Metadata 更新——O(1)。head 和 tail 就像两个不断外扩的指针，标记着 List 的有效范围。Pop 不会"回收"索引——head 只增不减（LPOP 时），tail 只减不增（RPOP 时），确保不会与新 push 的元素冲突。

## 随机访问：LINDEX 的 O(1) 优势

这是 NeoKV 相比 Redis 原生实现的一个有趣优势。Redis 原生用双向链表（后来是 ziplist/listpack + quicklist）实现 List，LINDEX 需要从头或尾遍历到目标位置，时间复杂度是 O(n)。NeoKV 用 uint64 索引作为 RocksDB 的 key，LINDEX 可以直接计算出目标位置的 key，一次 Get 就完成——O(1)。

```cpp
// LindexCommandHandler
int64_t index = parse_index(args[2]);
if (index < 0) index += list_meta.size;   // -1 表示最后一个元素
if (index < 0 || index >= list_meta.size) {
    output->SetNullString();
    return;
}

uint64_t actual_index = list_meta.head + static_cast<uint64_t>(index);
std::string subkey = RedisSubkeyKey::encode(..., encode_list_index(actual_index));
rocks->get(read_options, rocks->get_data_handle(), subkey, &value);
```

`head + logical_index` 就是实际的存储位置。逻辑索引 0 对应 head，逻辑索引 1 对应 head+1，以此类推。负数索引（-1 表示最后一个元素）先转换为正数再计算。这是双指针设计的直接红利——随机访问不需要遍历。

**LRANGE** 返回指定范围的元素。计算方式相同——把逻辑范围 `[start, stop]` 转换为实际索引范围 `[head+start, head+stop]`，逐个读取。这比 Redis 原生的链表遍历高效得多，虽然多次 `rocks->get()` 的总开销取决于数据是否在 block cache 中。

**LSET** 直接覆盖指定位置的元素——计算出实际索引，一次 Put 搞定，O(1)。

**LPOS** 查找元素在 List 中的位置，需要线性扫描——这个操作没有捷径，必须逐个比较，O(n)。

## 代价高昂的中间操作

双指针设计让两端操作极快，但中间操作就没那么优雅了。

**LINSERT** 在指定元素前/后插入新元素。问题在于：插入一个元素后，它后面的所有元素都需要向后移动一位，给新元素腾出位置：

```cpp
case pb::REDIS_LINSERT: {
    // 1. 线性扫描找到 pivot 元素的位置
    int64_t insert_pos = /* ... */;

    // 2. 从后往前，逐个把元素向后搬移一位
    for (int64_t i = static_cast<int64_t>(list_meta.size) - 1; i >= insert_pos; --i) {
        uint64_t old_idx = list_meta.head + static_cast<uint64_t>(i);
        uint64_t new_idx = old_idx + 1;
        // 读位置 i 的元素，写到位置 i+1
        rocks->get(ro, rocks->get_data_handle(), old_subkey, &val);
        batch.Put(rocks->get_data_handle(), new_subkey, val);
    }

    // 3. 在 insert_pos 写入新元素
    // 4. tail++ , size++
    break;
}
```

这是 O(n) 操作——最坏情况下（在 List 头部插入）需要搬移所有元素。每个元素需要一次 Get + 一次 Put，对于长 List 来说开销显著。

**LREM** 更加激进。删除匹配的元素后，为了保持索引的连续性（不留空洞），NeoKV 选择**重建整个 List**：

```cpp
case pb::REDIS_LREM: {
    // 1. 读取所有元素
    // 2. 标记要删除的元素
    // 3. 删除所有旧子键
    // 4. 用新的连续索引重写剩余元素
    uint64_t new_head = UINT64_MAX / 2;
    uint64_t new_tail = new_head + remaining.size();
    for (size_t i = 0; i < remaining.size(); ++i) {
        uint64_t idx = new_head + i;
        // 用新索引写入
    }
    list_meta.head = new_head;
    list_meta.tail = new_tail;
    list_meta.size = remaining.size();
    break;
}
```

LREM 直接把 head 和 tail 重置回 `UINT64_MAX / 2`，从头开始用连续索引重写所有剩余元素。为什么要重建而不是就地删除（留下空洞）？因为空洞会破坏 LINDEX 的 O(1) 语义——如果索引不连续，`head + logical_index` 不再能直接定位到正确的元素，LINDEX 就退化为需要跳过空洞的线性扫描。为了保住随机访问的 O(1)，LREM 付出了 O(n) 的重建代价。这是一个工程权衡。

**LTRIM** 只保留指定范围内的元素，删除范围外的。实现上类似——删除 `[head, head+start)` 和 `[head+stop+1, tail)` 范围的子键，更新 head 和 tail。

## 跨 List 的原子操作

**LMOVE** 原子地从一个 List 弹出元素并推入另一个：

```
LMOVE source destination LEFT|RIGHT LEFT|RIGHT
```

实现上就是"从 source 执行 LPOP/RPOP + 向 destination 执行 LPUSH/RPUSH"，全部在一个 WriteBatch 中。source 和 destination 可以是同一个 List——这就变成了旋转操作（比如 `LMOVE mylist mylist LEFT RIGHT` 把第一个元素移到最后）。

**LMPOP** 从多个 List 中弹出元素——遍历 key 列表，从第一个非空 List 中弹出。

这些跨 key 操作要求所有 key 在同一个 slot 中。跨 slot 的原子操作不支持，会返回 CROSSSLOT 错误。

## 一个有趣的对比

NeoKV 的 List 实现和 Redis 原生的 List 各有优劣。Redis 原生用 quicklist（ziplist/listpack + 链表），内存效率高，两端操作 O(1)，但 LINDEX 是 O(n)。NeoKV 用 RocksDB 的有序 KV，LPUSH/RPUSH/LPOP/RPOP 同样 O(1)（一次 Put/Delete），但 LINDEX 是 O(1)——因为可以直接计算出 key。代价是 LINSERT/LREM 这类中间操作更昂贵（需要搬移或重建），而 Redis 原生的链表插入只需要修改指针，O(1)。

如果你的使用模式是"只在两端操作 + 偶尔随机访问"，NeoKV 的实现其实更优。如果你频繁做 LINSERT/LREM，NeoKV 的性能会差一些。不过在实际的 Redis 使用中，List 最常见的模式就是作为队列——RPUSH + LPOP——这恰好是双指针设计的甜区。

## 检验你的理解

- head/tail 初始值为 `UINT64_MAX / 2`。如果一个 List 只做 LPUSH，理论上最多能存多少个元素？这个限制在实际中会成为问题吗？
- LREM 选择重建整个 List 而不是留下空洞。如果选择留下空洞，LINDEX 会怎样受影响？有没有折中方案？
- LINSERT 需要搬移后续元素。如果 pivot 在 List 的末尾附近，搬移的元素很少；如果 pivot 在开头，需要搬移几乎所有元素。有没有办法优化？（提示：考虑往另一个方向搬移）
- LMOVE 涉及两个 key。如果它们在不同的 slot，NeoKV 返回 CROSSSLOT 错误。但 Redis Cluster 也有同样的限制——这是分布式系统的固有约束，还是可以解决的？
- NeoKV 的 LINDEX 是 O(1)，Redis 原生是 O(n)。但 Redis 7.0 用了 listpack 替换 ziplist，在小 List 上 LINDEX 也很快（连续内存 + CPU cache 友好）。NeoKV 的 O(1) 优势在什么规模的 List 上才显现？

---

> 下一章：[12-ZSet 类型](./12-ZSet类型.md) — 我们将了解最复杂的数据类型：双 CF 索引、Score 的字节序保持编码，以及排名查询在 LSM-Tree 上的固有限制。
