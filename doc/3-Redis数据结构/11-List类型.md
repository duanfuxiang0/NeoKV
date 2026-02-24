# List 类型

## 本章概览

在这一章中，我们将了解：

- List 的存储模型：双指针（head/tail）实现 O(1) 两端操作
- Push/Pop 命令：LPUSH/RPUSH/LPOP/RPOP 的指针移动
- 随机访问命令：LINDEX/LRANGE 的索引计算
- 修改命令：LINSERT/LREM/LTRIM 的 O(n) 操作
- 高级命令：LMOVE/LMPOP 的跨 key 原子操作

List 是五种数据类型中存储设计最独特的——它使用 uint64 索引作为子键，通过 head/tail 指针实现双端队列语义。

**关键文件**：
- `src/redis/redis_service.cpp` — LpushCommandHandler、LpopCommandHandler 等
- `src/redis/region_redis.cpp` — REDIS_LPUSH、REDIS_RPUSH、REDIS_LPOP 等
- `include/redis/redis_metadata.h` — RedisListMetadata（额外的 head/tail 字段）

## 1. 存储模型

List 的 Metadata 比其他复合类型多了 head 和 tail 两个指针：

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

### 1.1 双指针设计

head 和 tail 都是 uint64，初始值为 `UINT64_MAX / 2`（约 9.2 × 10^18）：

```cpp
RedisListMetadata::RedisListMetadata(bool generate_version)
    : RedisMetadata(kRedisList, generate_version),
      head(UINT64_MAX / 2), tail(UINT64_MAX / 2) {}
```

从中间开始，允许向两端增长：

```
                    UINT64_MAX/2
                         │
    ◄── LPUSH 方向 ──── │ ──── RPUSH 方向 ──►
                         │
    [elem3][elem2][elem1]│[elem4][elem5][elem6]
     head-2 head-1 head  tail  tail+1 tail+2
```

- **LPUSH**：`head--`，在 head 位置写入元素
- **RPUSH**：在 tail 位置写入元素，`tail++`
- **LPOP**：读取 head 位置的元素，删除，`head++`
- **RPOP**：`tail--`，读取 tail 位置的元素，删除

所有 push/pop 操作都是 O(1)——只需要一次 RocksDB Put/Delete 加一次 Metadata 更新。

### 1.2 索引编码

子键中的 index 使用大端 uint64 编码：

```cpp
auto subkey = RedisSubkeyKey(region_id, index_id, slot,
                              user_key, metadata.version,
                              encode_u64_be(index)).encode();
```

大端编码保证 RocksDB 中的字节序与数值序一致，前缀迭代时元素按索引顺序排列。

## 2. Push/Pop 命令

### 2.1 LPUSH / RPUSH

```cpp
case pb::REDIS_LPUSH: {
    RedisListMetadata metadata;
    bool key_exists = read_list_metadata(key, &metadata);

    if (!key_exists || metadata.is_expired()) {
        metadata = RedisListMetadata(true);  // 新建，head=tail=UINT64_MAX/2
    }

    for (auto& element : request.list_elements()) {
        // LPUSH: 先递减 head，再写入
        metadata.head--;
        auto subkey = RedisSubkeyKey(..., metadata.version,
                                      encode_u64_be(metadata.head)).encode();
        batch.Put(data_cf, subkey, element);
        metadata.size++;
    }

    batch.Put(metadata_cf, meta_key, metadata.encode());
    db->Write(write_options, &batch);
    response->set_affected_rows(metadata.size);  // 返回 list 总长度
    break;
}

case pb::REDIS_RPUSH: {
    // 类似，但在 tail 位置写入，然后 tail++
    for (auto& element : request.list_elements()) {
        auto subkey = RedisSubkeyKey(..., metadata.version,
                                      encode_u64_be(metadata.tail)).encode();
        batch.Put(data_cf, subkey, element);
        metadata.tail++;
        metadata.size++;
    }
    // ...
}
```

注意 LPUSH 多个元素时的顺序：`LPUSH mylist a b c` 结果是 `[c, b, a, ...]`，与 Redis 行为一致。

### 2.2 LPOP / RPOP

```cpp
case pb::REDIS_LPOP: {
    RedisListMetadata metadata;
    if (!read_list_metadata(key, &metadata) || metadata.is_expired()) {
        break;  // 空 list
    }

    int pop_count = std::min(count, (int)metadata.size);
    std::vector<std::string> popped;

    for (int i = 0; i < pop_count; i++) {
        // 读取 head 位置的元素
        auto subkey = RedisSubkeyKey(..., metadata.version,
                                      encode_u64_be(metadata.head)).encode();
        std::string value;
        db->Get(read_options, data_cf, subkey, &value);
        popped.push_back(value);

        // 删除并移动 head
        batch.Delete(data_cf, subkey);
        metadata.head++;
        metadata.size--;
    }

    if (metadata.size <= 0) {
        batch.Delete(metadata_cf, meta_key);
    } else {
        batch.Put(metadata_cf, meta_key, metadata.encode());
    }

    db->Write(write_options, &batch);
    // 返回弹出的元素
    break;
}
```

## 3. 随机访问命令

### 3.1 LINDEX

通过索引直接访问元素，O(1)：

```cpp
// LindexCommandHandler::Run()
void Run() {
    RedisListMetadata metadata;
    if (!read_list_metadata(key, &metadata) || metadata.is_expired()) {
        output->SetNullString();
        return;
    }

    // 处理负数索引：-1 表示最后一个元素
    int64_t index = parse_index(args[2]);
    if (index < 0) index += metadata.size;
    if (index < 0 || index >= metadata.size) {
        output->SetNullString();
        return;
    }

    // 计算实际的 uint64 索引
    uint64_t real_index = metadata.head + index;

    auto subkey = RedisSubkeyKey(..., metadata.version,
                                  encode_u64_be(real_index)).encode();
    std::string value;
    if (db->Get(read_options, data_cf, subkey, &value).ok()) {
        output->SetBulkString(value);
    } else {
        output->SetNullString();
    }
}
```

`head + index` 就是实际的存储位置。这是双指针设计的优势——随机访问不需要遍历。

### 3.2 LRANGE

返回指定范围的元素：

```cpp
// LrangeCommandHandler::Run()
void Run() {
    // 规范化 start/stop 索引
    int64_t start = normalize(args[2], metadata.size);
    int64_t stop = normalize(args[3], metadata.size);

    std::vector<std::string> results;
    for (int64_t i = start; i <= stop; i++) {
        uint64_t real_index = metadata.head + i;
        auto subkey = RedisSubkeyKey(..., metadata.version,
                                      encode_u64_be(real_index)).encode();
        std::string value;
        if (db->Get(read_options, data_cf, subkey, &value).ok()) {
            results.push_back(value);
        }
    }
    output->SetArray(results);
}
```

### 3.3 LPOS

查找元素在 List 中的位置，需要线性扫描：

```cpp
// LposCommandHandler::Run()
void Run() {
    auto prefix = RedisSubkeyKey(..., metadata.version, "").encode_prefix();
    auto* iter = db->NewIterator(read_options, data_cf);

    int64_t pos = 0;
    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
        if (iter->value() == target) {
            output->SetInteger(pos);
            return;
        }
        pos++;
    }
    output->SetNullString();  // 未找到
}
```

## 4. 修改命令

### 4.1 LSET

直接覆盖指定位置的元素，O(1)：

```cpp
case pb::REDIS_LSET: {
    uint64_t real_index = metadata.head + index;
    auto subkey = RedisSubkeyKey(..., metadata.version,
                                  encode_u64_be(real_index)).encode();

    // 检查位置是否有效
    std::string old_value;
    if (!db->Get(read_options, data_cf, subkey, &old_value).ok()) {
        response->set_errmsg("index out of range");
        return;
    }

    batch.Put(data_cf, subkey, new_value);
    db->Write(write_options, &batch);
    break;
}
```

### 4.2 LINSERT

在指定元素前/后插入新元素。这是 O(n) 操作——需要移动后续元素：

```cpp
case pb::REDIS_LINSERT: {
    // 1. 找到 pivot 元素的位置
    int64_t pivot_pos = find_element(metadata, pivot);
    if (pivot_pos < 0) {
        response->set_affected_rows(-1);  // pivot 不存在
        break;
    }

    // 2. 确定插入位置
    int64_t insert_pos = (before ? pivot_pos : pivot_pos + 1);

    // 3. 将 insert_pos 之后的所有元素向后移动一位
    for (int64_t i = metadata.size - 1; i >= insert_pos; i--) {
        // 读取位置 i 的元素，写入位置 i+1
        // ...
    }

    // 4. 在 insert_pos 写入新元素
    // 5. 更新 tail 和 size
    metadata.tail++;
    metadata.size++;
    break;
}
```

### 4.3 LREM

删除匹配的元素，然后重新整理索引：

```cpp
case pb::REDIS_LREM: {
    // count > 0: 从头到尾删除 count 个匹配元素
    // count < 0: 从尾到头删除 |count| 个
    // count = 0: 删除所有匹配元素

    // 1. 扫描并标记要删除的位置
    // 2. 删除标记的子键
    // 3. 重新整理剩余元素的索引（填补空洞）
    // 4. 更新 head/tail/size
}
```

### 4.4 LTRIM

只保留指定范围内的元素：

```cpp
case pb::REDIS_LTRIM: {
    // 删除 [head, head+start) 和 [head+stop+1, tail) 范围的元素
    // 更新 head = head + start, tail = head + stop + 1
    // 更新 size = stop - start + 1
}
```

## 5. 高级命令

### 5.1 LMOVE

原子地从一个 List 弹出元素并推入另一个 List：

```
LMOVE source destination LEFT|RIGHT LEFT|RIGHT
```

```cpp
case pb::REDIS_LMOVE: {
    // 1. 从 source 弹出（LEFT=LPOP, RIGHT=RPOP）
    std::string element = pop_from(source, direction_from);

    // 2. 推入 destination（LEFT=LPUSH, RIGHT=RPUSH）
    push_to(destination, element, direction_to);

    // 3. 更新两个 List 的 Metadata
    // 全部在一个 WriteBatch 中
    db->Write(write_options, &batch);

    // 返回移动的元素
    response->set_errmsg(element);
    break;
}
```

source 和 destination 可以是同一个 List（旋转操作）。

### 5.2 LMPOP

从多个 List 中弹出元素（从第一个非空 List 弹出）：

```
LMPOP numkeys key1 key2 ... LEFT|RIGHT [COUNT count]
```

## 6. 已实现命令一览

| 命令 | 类型 | 复杂度 | 说明 |
|------|------|--------|------|
| LPUSH / RPUSH | 写 | O(1) per element | 两端推入 |
| LPOP / RPOP | 写 | O(1) per element | 两端弹出 |
| LLEN | 读 | O(1) | 返回长度 |
| LINDEX | 读 | O(1) | 按索引访问 |
| LRANGE | 读 | O(n) | 范围查询 |
| LPOS | 读 | O(n) | 查找元素位置 |
| LSET | 写 | O(1) | 按索引设置 |
| LINSERT | 写 | O(n) | 在指定元素前/后插入 |
| LREM | 写 | O(n) | 删除匹配元素 |
| LTRIM | 写 | O(n) | 裁剪 List |
| LMOVE | 写 | O(1) | 跨 List 原子移动 |
| LMPOP | 写 | O(1) | 从多个 List 弹出 |

## 检验你的理解

- head/tail 初始值为 `UINT64_MAX / 2`。如果一个 List 只做 LPUSH，理论上最多能存多少个元素？这个限制在实际中会成为问题吗？
- LINSERT 需要移动后续元素，是 O(n) 操作。有没有更好的数据结构可以实现 O(log n) 的插入？代价是什么？
- LREM 删除元素后需要重新整理索引。如果不整理（留下空洞），会有什么问题？
- LMOVE 涉及两个 key。如果它们在不同的 slot，NeoKV 如何处理？
- List 的 LINDEX 是 O(1)，而 Redis 原生的 LINDEX 也是 O(n)（链表实现）。NeoKV 的实现在这一点上反而更优，为什么？

---

> 下一章：[12-ZSet 类型](./12-ZSet类型.md) — 我们将了解最复杂的数据类型：双 CF 索引、Score 编码和范围查询。
