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

// Simplified version for neo-redis - SQL features removed

#pragma once

#include <cstddef>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <bthread/execution_queue.h>
#include "common.h"
#include "proto/meta.interface.pb.h"
#include "proto/common.pb.h"

namespace neokv {

// TTLInfo definition - also defined in transaction_pool.h, use #ifndef guard
#ifndef NEOKV_TTLINFO_DEFINED
#define NEOKV_TTLINFO_DEFINED
struct TTLInfo {
	int64_t ttl_duration_s = 0;
	int64_t online_ttl_expire_time_us = 0;
};
#endif

typedef std::map<std::string, int64_t> StrInt64Map;

struct RegionInfo {
	pb::RegionInfo region_info;
	RegionInfo() {
	}
	explicit RegionInfo(const RegionInfo& other) {
		region_info = other.region_info;
	}
};

struct TableRegionInfo {
	// region_id => RegionInfo
	std::unordered_map<int64_t, RegionInfo> region_info_mapping;
	// partition map of (partition_id => (start_key => regionid))
	std::unordered_map<int64_t, StrInt64Map> key_region_mapping;

	void update_leader(int64_t region_id, const std::string& leader) {
		if (region_info_mapping.count(region_id) == 1) {
			region_info_mapping[region_id].region_info.set_leader(leader);
		}
	}

	int get_region_info(int64_t region_id, pb::RegionInfo& info) {
		if (region_info_mapping.count(region_id) == 1) {
			info = region_info_mapping[region_id].region_info;
			return 0;
		}
		return -1;
	}

	void insert_region_info(const pb::RegionInfo& info) {
		if (region_info_mapping.count(info.region_id()) == 1) {
			region_info_mapping[info.region_id()].region_info = info;
		} else {
			region_info_mapping[info.region_id()].region_info = info;
		}
	}
};

// Minimal IndexInfo for Redis - just type info
struct IndexInfo {
	int64_t id = 0;
	int64_t version = 0;
	pb::IndexType type = pb::I_PRIMARY;
	pb::SegmentType segment_type = pb::S_DEFAULT;
	pb::IndexState state = pb::IS_PUBLIC; // Index state for compatibility
	std::string name;
	std::string short_name;

	bool is_global = false;
	bool is_partitioned = false;
};
typedef std::shared_ptr<IndexInfo> SmartIndex;

// Schema configuration stub for neo-redis
struct SchemaConf {
	int32_t pk_prefix_balance() const {
		return 0;
	} // No prefix balance in neo-redis
};

// Minimal TableInfo for Redis
struct TableInfo {
	int64_t id = 0;
	int64_t version = 0;
	int64_t partition_num = 1;
	std::string name;
	std::string short_name;
	std::string namespace_;
	pb::Charset charset = pb::UTF8;

	std::vector<int64_t> indices;

	// Stub for sign blacklist (SQL feature, not used in neo-redis)
	std::set<uint64_t> sign_blacklist;

	// Schema conf stub
	SchemaConf schema_conf;
};
typedef std::shared_ptr<TableInfo> SmartTable;

class SchemaFactory {
public:
	static SchemaFactory* get_instance() {
		static SchemaFactory instance;
		return &instance;
	}

	int init() {
		return 0;
	}

	// For Redis: register a simple index without SQL table schema
	void register_redis_index(int64_t index_id) {
		std::lock_guard<std::mutex> lock(_mutex);
		auto index_info = std::make_shared<IndexInfo>();
		index_info->id = index_id;
		index_info->type = pb::I_PRIMARY;
		index_info->name = "redis_primary";
		_index_info_mapping[index_id] = index_info;
	}

	SmartIndex get_index_info_ptr(int64_t index_id) {
		std::lock_guard<std::mutex> lock(_mutex);
		auto it = _index_info_mapping.find(index_id);
		if (it != _index_info_mapping.end()) {
			return it->second;
		}
		return nullptr;
	}

	SmartTable get_table_info_ptr(int64_t table_id) {
		std::lock_guard<std::mutex> lock(_mutex);
		auto it = _table_info_mapping.find(table_id);
		if (it != _table_info_mapping.end()) {
			return it->second;
		}
		return nullptr;
	}

	int get_table_id(const std::string& full_name, int64_t& table_id) {
		std::lock_guard<std::mutex> lock(_mutex);
		auto it = _table_name_id_mapping.find(full_name);
		if (it != _table_name_id_mapping.end()) {
			table_id = it->second;
			return 0;
		}
		return -1;
	}

	bool exist_tableid(int64_t table_id) {
		std::lock_guard<std::mutex> lock(_mutex);
		return _table_info_mapping.count(table_id) > 0;
	}

	// Get TTL duration for a table (stub - not used in neo-redis)
	// Returns TTLInfo struct for compatibility
	TTLInfo get_ttl_duration(int64_t /*table_id*/) {
		TTLInfo info;
		info.ttl_duration_s = 0;
		info.online_ttl_expire_time_us = false;
		return info;
	}

	// Get index info (returns -1 if not found, for compatibility)
	int get_index_info(int64_t index_id, IndexInfo& info) {
		std::lock_guard<std::mutex> lock(_mutex);
		auto it = _index_info_mapping.find(index_id);
		if (it != _index_info_mapping.end() && it->second) {
			info = *(it->second);
			return 0;
		}
		return -1;
	}

	// Alternative signature: returns IndexInfo directly (stub)
	IndexInfo get_index_info(int64_t index_id) {
		std::lock_guard<std::mutex> lock(_mutex);
		auto it = _index_info_mapping.find(index_id);
		if (it != _index_info_mapping.end() && it->second) {
			return *(it->second);
		}
		return IndexInfo(); // Return empty IndexInfo if not found
	}

	// Get table info - stub for neo-redis
	TableInfo get_table_info(int64_t table_id) {
		std::lock_guard<std::mutex> lock(_mutex);
		auto it = _table_info_mapping.find(table_id);
		if (it != _table_info_mapping.end() && it->second) {
			return *(it->second);
		}
		TableInfo info;
		info.id = -1; // Indicate not found
		return info;
	}

	// Get separate switch - stub for neo-redis (no storage-compute separation)
	bool get_separate_switch(int64_t /*table_id*/) {
		return false; // Neo-redis doesn't use storage-compute separation
	}

	// Is OLAP table - stub for neo-redis
	bool is_olap_table(int64_t /*table_id*/, int64_t /*partition_id*/ = 0, bool* is_cold = nullptr) {
		if (is_cold)
			*is_cold = false;
		return false; // Neo-redis doesn't use OLAP
	}

	// Update tables from heartbeat response - stub for neo-redis
	void update_tables_double_buffer_sync(const google::protobuf::RepeatedPtrField<pb::SchemaInfo>& /*schema_info*/) {
		// Stub - neo-redis doesn't use SQL schema updates
	}

	// Get region capacity - stub for neo-redis
	int64_t get_region_capacity(int64_t /*table_id*/) {
		return 100 * 1024 * 1024; // Default 100MB
	}

	int64_t get_region_capacity(int64_t /*table_id*/, int64_t& /*split_lines*/) {
		return 100 * 1024 * 1024; // Default 100MB
	}

	// Get tail split nums - stub for neo-redis
	int64_t get_tail_split_nums(int64_t /*table_id*/) {
		return 1; // Default split into 1 region
	}

	// Check if in fast importer mode - stub for neo-redis
	bool is_in_fast_importer(int64_t /*table_id*/) {
		return false; // Not in fast import mode
	}

	// Get merge switch - stub for neo-redis
	bool get_merge_switch(int64_t /*table_id*/) {
		return false; // No merge in neo-redis
	}

	// Update table - stub for neo-redis
	void update_table(const pb::SchemaInfo& /*schema_info*/) {
		// Stub - neo-redis doesn't use SQL tables
	}

	// Get all table versions - stub for neo-redis
	void get_all_table_version(std::unordered_map<int64_t, int64_t>& /*table_versions*/) {
		// Stub - returns empty map
	}

	// Region info management
	void update_region(const pb::RegionInfo& region) {
		std::lock_guard<std::mutex> lock(_mutex);
		int64_t table_id = region.table_id();
		_table_region_mapping[table_id].insert_region_info(region);
	}

	int get_region_info(int64_t table_id, int64_t region_id, pb::RegionInfo& info) {
		std::lock_guard<std::mutex> lock(_mutex);
		auto it = _table_region_mapping.find(table_id);
		if (it == _table_region_mapping.end()) {
			return -1;
		}
		return it->second.get_region_info(region_id, info);
	}

private:
	SchemaFactory() = default;
	~SchemaFactory() = default;

	std::mutex _mutex;
	std::unordered_map<int64_t, SmartIndex> _index_info_mapping;
	std::unordered_map<int64_t, SmartTable> _table_info_mapping;
	std::unordered_map<std::string, int64_t> _table_name_id_mapping;
	std::unordered_map<int64_t, TableRegionInfo> _table_region_mapping;
};

} // namespace neokv
