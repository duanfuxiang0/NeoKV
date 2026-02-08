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
// Redis ID conventions
// ============================================================================
//
// Redis data in neo-redis uses RegionInfo.table_id as both table_id/index_id
// for key encoding and routing. There is no globally fixed hard-coded table id.
//
// The constants below are kept only for legacy compatibility; runtime logic
// must not depend on them. A value of 0 means "dynamic/resolved from region".
constexpr int64_t REDIS_TABLE_ID = 0;
constexpr int64_t REDIS_INDEX_ID = 0;

// Redis slot 数量 (与 Redis Cluster 一致)
constexpr uint16_t REDIS_SLOT_COUNT = 16384;

// 检查一个 table_id 是否是 Redis 表
inline bool is_redis_table(int64_t table_id) {
	return table_id > 0;
}

// 检查一个 index_id 是否是 Redis 索引
inline bool is_redis_index(int64_t index_id) {
	return index_id > 0;
}

} // namespace neokv
