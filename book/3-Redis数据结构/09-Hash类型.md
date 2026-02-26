# Hash 类型

## 概览

- Hash 的存储模型：Metadata + Subkey 的双层结构
- HSET/HDEL 如何维护 size 计数
- 前缀迭代：HGETALL 如何高效遍历所有 field
- Version-Based Lazy Deletion 在 Hash 上的实际运作

Hash 是 NeoKV 中第一个复合类型。理解了它的实现模式——Metadata 记元信息、Subkey 存数据、size 实时维护、version 隔离删除——后面的 Set、List、ZSet 就很容易举一反三。

## 从 String 到 Hash：复杂度的跃升

上一章我们看到 String 是"单 KV 类型"，值内联在 Metadata 中，一次读写就搞定。Hash 则是另一个世界。一个 Hash key 可能有几个 field，也可能有几百万个 field。你不可能把所有 field 塞进一条 Metadata 记录里——那样 GET 一条 Metadata 就要读几百 MB 的数据。

所以 Hash 使用了两层结构：**Metadata CF 存元信息，DATA CF 存子键**。

```
REDIS_METADATA_CF:
  Key:   [prefix]["mykey"]
  Value: [flags=Hash][expire_ms][version=100][size=3]

DATA_CF:
  Key:   [prefix]["mykey"][version=100]["name"]   → "Alice"
  Key:   [prefix]["mykey"][version=100]["age"]    → "30"
  Key:   [prefix]["mykey"][version=100]["city"]   → "Beijing"
```

Metadata 记录了类型（Hash）、过期时间、版本号和 field 数量。每个 field 是 DATA CF 中的一条记录，key 中包含 version（用于 Lazy Deletion），value 就是 field 的值。

这个 `size` 字段值得专门说一下。如果没有它，HLEN 需要遍历所有子键来计数——对于百万 field 的 Hash 来说是灾难性的 O(n)。通过在 Metadata 中维护 size，HLEN 变成了一次 RocksDB Get，O(1)。代价是每次 HSET/HDEL 都需要额外更新一次 Metadata（多一次 Put）。但这个代价是值得的——HLEN 的调用频率通常远高于写入频率，用写入时的少量开销换取读取时的巨大收益，是一个合理的工程权衡。

## HSET：复合类型写入的标准模式

HSET 可以一次设置多个 field：`HSET mykey name Alice age 30 city Beijing`。它的 Apply 层实现展示了复合类型写入的标准模式——读 Metadata、处理子键、更新 Metadata、原子提交：

```cpp
case pb::REDIS_HSET: {
    RedisMetadata metadata;
    bool key_exists = read_metadata(rocks, _region_id, index_id, slot, user_key, &metadata);

    if (!key_exists || metadata.is_expired() || metadata.type != kRedisHash) {
        metadata = RedisMetadata(kRedisHash, true);  // generate_version=true
        metadata.size = 0;
    }

    int new_fields = 0;
    for (auto& field : redis_req.fields()) {
        auto subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, 
                                              user_key, metadata.version, field.field());
        std::string old_value;
        auto s = rocks->get(read_options, rocks->get_data_handle(), subkey, &old_value);
        if (!s.ok()) {
            metadata.size++;
            new_fields++;
        }
        batch.Put(rocks->get_data_handle(), subkey, field.value());
    }

    batch.Put(rocks->get_redis_metadata_handle(), meta_key, metadata.encode());
    rocks->write(write_options, &batch);
    response->set_affected_rows(new_fields);
    break;
}
```

几个细节值得注意。

首先，如果 key 不存在（或已过期、或类型不匹配），会创建一个全新的 `RedisMetadata`，并调用 `generate_version=true` 来生成新版本号。这个新版本号保证了即使之前同名 key 的旧子键还残留在 DATA CF 中，新写入的子键也不会和它们冲突——因为 version 不同。

其次，对于每个 field，在写入之前先检查它是否已存在。这不是为了防止覆盖（覆盖是允许的），而是为了**正确维护 size 计数**——只有新增的 field 才让 size 加一，更新已有 field 不改变 size。这也是为什么 HSET 返回的是**新增** field 的数量，而不是修改的数量。如果你执行 `HSET mykey name Bob`（name 已存在），返回值是 0——更新了值，但没有新增 field。

最后，所有修改都在同一个 WriteBatch 中——子键的写入和 Metadata 的更新是原子的，不会出现"子键写了但 Metadata 没更新"导致 size 不一致的情况。

## HDEL：size 降为 0 时的特殊处理

HDEL 的模式与 HSET 对称——检查 field 是否存在、删除子键、递减 size：

```cpp
case pb::REDIS_HDEL: {
    int deleted = 0;
    for (auto& field : redis_req.fields()) {
        auto subkey = RedisSubkeyKey::encode(..., metadata.version, field);
        std::string old_value;
        if (rocks->get(read_options, rocks->get_data_handle(), subkey, &old_value).ok()) {
            batch.Delete(rocks->get_data_handle(), subkey);
            deleted++;
        }
    }
    current_size = (current_size > deleted) ? (current_size - deleted) : 0;

    if (current_size == 0) {
        batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
    } else {
        // 更新 Metadata 中的 size
        batch.Put(rocks->get_redis_metadata_handle(), meta_key, metadata.encode());
    }
    break;
}
```

当 `size` 降为 0 时，Metadata 也被删除——**空 Hash 不应该存在**。这与 Redis 的行为一致：当一个 Hash 的所有 field 都被删除后，key 本身也消失了。`EXISTS mykey` 返回 0，`TYPE mykey` 返回 none。size 的计算用了 `(current_size > deleted) ? (current_size - deleted) : 0` 而不是简单的减法，这是一种防御性编程——即使由于某种原因 size 与实际 field 数不完全一致（比如数据修复场景），也不会让 size 变成一个巨大的负数。

**HSETNX** 只在 field 不存在时设置。实现很直接：构建子键 key，用 `rocks->get()` 检查存在性，存在就跳过，不存在就写入并递增 size。

**HINCRBY** 是 Hash 版的 INCR——读-改-写模式，与 String 的 INCR 几乎一样。读取 field 的当前值，解析为整数，加上增量，写回。如果 field 不存在，从 0 开始计算，同时递增 size（因为是新 field）。返回值通过 `num_affected` 传递。

## 读命令：前缀迭代的威力

**HGET** 是最简单的读操作——构建子键 key（prefix + version + field），一次 `rocks->get()` 就完成了。先读 Metadata 获取 version 和类型信息，再用 version 构建完整的子键 key 去 DATA CF 查找。

**HGETALL** 才是展示 RocksDB 前缀迭代能力的地方。它需要遍历一个 Hash 的所有 field，实现方式是构建一个前缀，然后用 Iterator 从这个前缀开始扫描：

```cpp
std::string prefix = RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), 
                                                    route.slot, user_key, meta.version);
rocksdb::ReadOptions read_options;
read_options.prefix_same_as_start = true;
std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix)) break;
    std::string field_name(iter->key().data() + prefix.size(), 
                           iter->key().size() - prefix.size());
    std::string field_value(iter->value().data(), iter->value().size());
    pairs.emplace_back(std::move(field_name), std::move(field_value));
}
```

`prefix_same_as_start = true` 告诉 RocksDB 启用前缀优化——当迭代器发现当前 key 不再匹配前缀时，可以提前终止而不是继续在整个 SST 文件中扫描。前缀包含 `[region_id][index_id][slot][user_key][version]`，这保证了：只遍历属于这个 Hash 的、当前 version 的子键，不会扫到其他 key 的数据，也不会扫到旧 version 的孤儿子键。

field name 通过从完整 key 中去掉前缀部分来提取——前缀之后的所有字节就是 field name 本身。这种"前缀 = 定位，后缀 = 数据"的编码设计贯穿了所有复合类型。

**HKEYS** 只返回 field name（丢弃 value），**HVALS** 只返回 value（丢弃 field name），**HGETALL** 两者都要。它们共享同一个前缀迭代逻辑，只是收集的内容不同。

**HLEN** 直接返回 Metadata 中的 size，O(1)。**HEXISTS** 构建子键 key，检查 `rocks->get()` 是否返回 OK。

**HRANDFIELD** 需要随机返回 field。在 RocksDB 上实现"随机"需要先收集所有 field 再随机选择——LSM-Tree 不支持高效的随机定位，这是存储引擎的固有特性决定的。

**HSCAN** 实现游标迭代。游标本质上编码了上次迭代停止的位置，下次从该位置继续。

## Lazy Deletion 实战

这是上一章讲过的理论在实际中的运作。当执行 `DEL mykey`（mykey 是一个有 100 万个 field 的 Hash）时，只需要删除 Metadata CF 中的一条记录，DATA CF 中的 100 万条子键原封不动。整个操作耗时微秒级，不会阻塞 Raft apply。

```
执行前：
  Metadata: mykey → {version=100, size=1000000}
  Data CF:  mykey/v100/field1 → value1
            mykey/v100/field2 → value2
            ... (100 万条)

执行 DEL mykey：
  只删除 Metadata 记录
  Data CF 中的 100 万条子键不动

执行后读取 HGET mykey field1：
  1. 查 Metadata → 不存在 → 返回 nil
  （根本不会去查 Data CF）

执行 HSET mykey field1 newvalue：
  1. 创建新 Metadata: {version=200, size=1}
  2. 写入子键: mykey/v200/field1 → newvalue
  3. 旧子键 mykey/v100/* 仍然存在，但 version 不匹配，永远不会被读到
```

旧 version 的子键变成了"孤儿"——它们占用磁盘空间但不影响正确性。最终在 RocksDB compaction 时被清理。这个过程是渐进式的，不会产生突发的 I/O 峰值。

这个机制让 DEL 的时间复杂度从 O(n) 变成了 O(1)，代价是延迟回收磁盘空间。在实际生产中，这个权衡几乎总是值得的——用户对 DEL 的延迟比对磁盘空间的敏感度高得多。

## 并发安全

如果两个客户端同时对同一个 Hash 执行 HSET（不同的 field），会有并发问题吗？不会。因为它们的请求都要经过 Raft，最终在 Apply 层串行执行。第一个 HSET 读到 size=3，加了一个 field，写回 size=4。第二个 HSET 读到 size=4，加了一个 field，写回 size=5。Raft 的串行 apply 保证了 size 的正确性。

但如果两个客户端对同一个 field 并发 HSET，最终只有一个会"赢"——后 apply 的那个覆盖前一个。这和 Redis 原生的行为一致：Hash field 没有 CAS（Compare-And-Swap）语义。

## 检验你的理解

- HSET 返回的是新增 field 数还是总修改数？这在 apply 层如何计算？Redis 为什么选择返回新增数？
- 删除一个有百万 field 的 Hash 时，Lazy Deletion 让 DEL 变成 O(1)。但孤儿子键占用的磁盘空间什么时候才能释放？如果 compaction 长时间不触发怎么办？
- HGETALL 需要遍历所有子键。如果一个 Hash 有 100 万个 field，这个操作的性能如何？有什么优化建议？（提示：Redis 原生的 HSCAN 就是为了解决这个问题）
- HDEL 中 size 的计算用了 `(current_size > deleted) ? (current_size - deleted) : 0`，而不是简单减法。这种防御性编程在什么场景下会发挥作用？
- 如果两个客户端同时对同一个 Hash 执行 HDEL（删除不同的 field），最终的 size 会正确吗？为什么？

---

> 下一章：[10-Set 类型](./10-Set类型.md) — 我们将了解 Set 的存储模型——member 即 key、value 为空的巧妙设计，以及集合运算在 RocksDB 上的实现。
