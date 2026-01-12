// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>

namespace neokv {

// ============================================================================
// Redis 静态配置 - 不需要创建 SQL 表
// ============================================================================

// 固定的 Redis table_id 和 index_id
// 这些值是硬编码的，不需要通过 SQL 创建表
// 选择一个不太可能与用户表冲突的值 (负数或很大的正数)
// NEOKV 的 table_id 通常从 1 开始递增，我们使用一个特殊的区间

// Redis 使用的 table_id (= index_id for primary key)
// 使用一个特殊的值，避免与用户表冲突
constexpr int64_t REDIS_TABLE_ID = 0x7FFFFFFFFF000001LL;  // 大正数，不会与普通表冲突
constexpr int64_t REDIS_INDEX_ID = REDIS_TABLE_ID;        // 对于 primary key，index_id = table_id

// Redis slot 数量 (与 Redis Cluster 一致)
constexpr uint16_t REDIS_SLOT_COUNT = 16384;

// 检查一个 table_id 是否是 Redis 表
inline bool is_redis_table(int64_t table_id) {
    return table_id == REDIS_TABLE_ID;
}

// 检查一个 index_id 是否是 Redis 索引
inline bool is_redis_index(int64_t index_id) {
    return index_id == REDIS_INDEX_ID;
}

} // namespace neokv

