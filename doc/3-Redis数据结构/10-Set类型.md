# Set 类型

## 本章概览

在这一章中，我们将了解：

- Set 的存储模型：member 即 key 的巧妙设计
- 基础命令：SADD/SREM/SISMEMBER/SMEMBERS/SCARD
- 随机操作：SPOP/SRANDMEMBER 在 RocksDB 上的实现
- 集合运算：SINTER/SUNION/SDIFF 的内存计算模式
- Store 类命令：SINTERSTORE/SUNIONSTORE/SDIFFSTORE 的 Raft 写入

**关键文件**：
- `src/redis/redis_service.cpp` — SaddCommandHandler、SinterCommandHandler 等
- `src/redis/region_redis.cpp` — REDIS_SADD、REDIS_SREM、REDIS_SPOP 等

## 1. 存储模型

Set 的子键设计非常简洁——**member 就是 sub_key，value 为空**：

```
REDIS_METADATA_CF:
  Key:   [prefix]["myset"]
  Value: [flags=Set][expire_ms][version=100][size=3]

DATA_CF:
  Key:   [prefix]["myset"][version=100]["alice"]   → ""  (空 value)
  Key:   [prefix]["myset"][version=100]["bob"]     → ""
  Key:   [prefix]["myset"][version=100]["charlie"] → ""
```

value 为空的好处：
- **节省空间**：member 信息完全在 key 中，不需要重复存储
- **存在性检查即成员检查**：`SISMEMBER` 只需要一次 `db->Get()` 判断 key 是否存在
- **天然有序**：RocksDB 按 key 排序，member 自然按字典序排列

## 2. 基础命令

### 2.1 SADD

```cpp
case pb::REDIS_SADD: {
    RedisMetadata metadata;
    bool key_exists = read_metadata(key, &metadata);

    if (!key_exists || metadata.is_expired() || metadata.type != kRedisSet) {
        metadata = RedisMetadata(kRedisSet, true);
        metadata.size = 0;
    }

    int added = 0;
    for (auto& member : request.members()) {
        auto subkey = RedisSubkeyKey(..., metadata.version, member).encode();

        // 检查 member 是否已存在
        std::string dummy;
        if (!db->Get(read_options, data_cf, subkey, &dummy).ok()) {
            batch.Put(data_cf, subkey, "");  // 空 value
            metadata.size++;
            added++;
        }
    }

    batch.Put(metadata_cf, meta_key, metadata.encode());
    db->Write(write_options, &batch);
    response->set_affected_rows(added);  // 返回新增数量
    break;
}
```

### 2.2 SREM

与 HDEL 模式相同：检查存在性 → 删除子键 → 更新 size → size=0 时删除 Metadata。

### 2.3 SISMEMBER / SMISMEMBER

```cpp
// SismemberCommandHandler::Run()
void Run() {
    // 读取 Metadata，检查过期
    auto subkey = RedisSubkeyKey(..., metadata.version, member).encode();
    std::string dummy;
    output->SetInteger(db->Get(read_options, data_cf, subkey, &dummy).ok() ? 1 : 0);
}
```

SMISMEMBER 批量检查多个 member，返回 0/1 数组。

### 2.4 SMEMBERS / SCARD

SMEMBERS 通过前缀迭代返回所有 member，SCARD 直接返回 `metadata.size`。

## 3. 随机操作

### 3.1 SPOP

随机弹出一个或多个 member。在 RocksDB 上实现"随机"需要一些技巧：

```cpp
case pb::REDIS_SPOP: {
    // 1. 前缀迭代收集所有 member
    std::vector<std::string> members;
    auto prefix = RedisSubkeyKey(..., metadata.version, "").encode_prefix();
    auto* iter = db->NewIterator(read_options, data_cf);
    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
        members.push_back(extract_member(iter->key()));
    }

    // 2. 随机选择 count 个
    std::shuffle(members.begin(), members.end(), rng);
    int pop_count = std::min(count, (int)members.size());

    // 3. 删除选中的 member
    for (int i = 0; i < pop_count; i++) {
        auto subkey = RedisSubkeyKey(..., metadata.version, members[i]).encode();
        batch.Delete(data_cf, subkey);
        metadata.size--;
    }

    // 4. 更新 Metadata
    if (metadata.size <= 0) {
        batch.Delete(metadata_cf, meta_key);
    } else {
        batch.Put(metadata_cf, meta_key, metadata.encode());
    }

    db->Write(write_options, &batch);
    // 返回弹出的 member 列表
    break;
}
```

SPOP 需要先收集所有 member 再随机选择，对于大 Set 来说性能不理想。这是 RocksDB 上实现随机访问的固有限制——LSM-Tree 不支持高效的随机定位。

### 3.2 SRANDMEMBER

与 SPOP 类似但不删除。支持负数 count（允许重复）。

## 4. 集合运算

### 4.1 只读运算：SINTER / SUNION / SDIFF

这些命令涉及多个 key，但只读不写，在 handler 层直接执行：

```cpp
// SinterCommandHandler::Run() — 简化逻辑
void Run() {
    // 1. 读取所有参与集合的 member
    std::vector<std::set<std::string>> all_sets;
    for (auto& key : keys) {
        std::set<std::string> members;
        // 前缀迭代收集 member
        collect_members(key, &members);
        all_sets.push_back(std::move(members));
    }

    // 2. 在内存中执行集合运算
    std::set<std::string> result = all_sets[0];
    for (size_t i = 1; i < all_sets.size(); i++) {
        std::set<std::string> temp;
        std::set_intersection(result.begin(), result.end(),
                              all_sets[i].begin(), all_sets[i].end(),
                              std::inserter(temp, temp.begin()));
        result = std::move(temp);
    }

    // 3. 返回结果
    output->SetArray(result);
}
```

SUNION 使用 `set_union`，SDIFF 使用 `set_difference`。

### 4.2 SINTERCARD

只返回交集大小，支持 LIMIT 提前终止：

```cpp
// 当交集大小达到 limit 时立即返回，避免不必要的计算
if (result.size() >= limit) break;
```

### 4.3 写入运算：SINTERSTORE / SUNIONSTORE / SDIFFSTORE

这些命令将运算结果写入目标 key，需要通过 Raft：

```cpp
case pb::REDIS_SINTERSTORE: {
    // 1. 读取所有源集合的 member（在 apply 层）
    // 2. 执行集合运算
    // 3. 如果目标 key 已存在，通过 Lazy Deletion 处理旧数据
    //    （生成新 version，旧子键变成孤儿）
    // 4. 写入结果到目标 key
    RedisMetadata new_meta(kRedisSet, true);  // 新 version
    new_meta.size = result.size();
    batch.Put(metadata_cf, dest_meta_key, new_meta.encode());

    for (auto& member : result) {
        auto subkey = RedisSubkeyKey(..., new_meta.version, member).encode();
        batch.Put(data_cf, subkey, "");
    }
    db->Write(write_options, &batch);
    break;
}
```

### 4.4 SMOVE

原子地将 member 从一个 Set 移动到另一个：

```cpp
case pb::REDIS_SMOVE: {
    // 1. 从源 Set 删除 member
    // 2. 向目标 Set 添加 member
    // 3. 更新两个 Set 的 Metadata
    // 全部在一个 WriteBatch 中，保证原子性
}
```

## 5. 已实现命令一览

| 命令 | 类型 | 说明 |
|------|------|------|
| SADD | 写 | 添加 member |
| SREM | 写 | 删除 member |
| SISMEMBER | 读 | 检查 member 是否存在 |
| SMISMEMBER | 读 | 批量检查 member |
| SMEMBERS | 读 | 返回所有 member |
| SCARD | 读 | 返回 member 数量（O(1)） |
| SPOP | 写 | 随机弹出 member |
| SRANDMEMBER | 读 | 随机返回 member（不删除） |
| SSCAN | 读 | 游标迭代 |
| SINTER | 读 | 交集 |
| SUNION | 读 | 并集 |
| SDIFF | 读 | 差集 |
| SINTERCARD | 读 | 交集大小（支持 LIMIT） |
| SMOVE | 写 | 原子移动 member |
| SINTERSTORE | 写 | 交集结果写入目标 key |
| SUNIONSTORE | 写 | 并集结果写入目标 key |
| SDIFFSTORE | 写 | 差集结果写入目标 key |

## 检验你的理解

- Set 的 value 为空，那 RocksDB 中这些空 value 会占用多少空间？（提示：考虑 SST 文件的编码开销）
- SPOP 需要先收集所有 member 再随机选择。如果 Set 有 100 万个 member，有什么更高效的实现方案？
- SINTERSTORE 如果目标 key 已存在且有大量 member，旧数据如何处理？会不会阻塞？
- 集合运算涉及多个 key。如果这些 key 不在同一个 slot，会发生什么？NeoKV 如何处理？
- SMOVE 需要同时修改两个 key。如果这两个 key 在不同的 Region，能否保证原子性？

---

> 下一章：[11-List 类型](./11-List类型.md) — 我们将了解 List 的双指针设计和 O(1) push/pop 实现。
