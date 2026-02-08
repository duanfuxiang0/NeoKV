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

#include "redis_router.h"
#include "redis_service.h"
#include "store.h"
#include "region.h"
#include "common.h"
#include "schema_factory.h"
#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <arpa/inet.h>

namespace {
// Simple big-endian conversion for slot encoding
inline uint16_t to_endian_u16(uint16_t val) {
	return ntohs(val);
}
} // anonymous namespace

namespace neokv {

namespace {

// Command key patterns
// first_key_index: 1-based index of first key, 0 means no keys
// last_key_index: -1 means all remaining args, positive number is specific index
// key_step: 1 for consecutive keys, 2 for key-value pairs
const std::unordered_map<std::string, RedisKeyPattern> kKeyPatterns = {
    // String commands
    {"get", {1, 1, 1}},
    {"set", {1, 1, 1}},
    {"setnx", {1, 1, 1}},
    {"setex", {1, 1, 1}},
    {"psetex", {1, 1, 1}},
    {"mget", {1, -1, 1}},
    {"mset", {1, -1, 2}}, // key value key value ...
    {"msetnx", {1, -1, 2}},
    {"del", {1, -1, 1}},
    {"unlink", {1, -1, 1}},
    {"exists", {1, -1, 1}},
    {"expire", {1, 1, 1}},
    {"expireat", {1, 1, 1}},
    {"pexpire", {1, 1, 1}},
    {"pexpireat", {1, 1, 1}},
    {"ttl", {1, 1, 1}},
    {"pttl", {1, 1, 1}},
    {"persist", {1, 1, 1}},
    {"type", {1, 1, 1}},
    {"getset", {1, 1, 1}},
    {"append", {1, 1, 1}},
    {"strlen", {1, 1, 1}},
    {"incr", {1, 1, 1}},
    {"decr", {1, 1, 1}},
    {"incrby", {1, 1, 1}},
    {"decrby", {1, 1, 1}},
    {"incrbyfloat", {1, 1, 1}},
    // Commands with no keys (handled specially)
    {"ping", {0, 0, 0}},
    {"echo", {0, 0, 0}},
    {"cluster", {0, 0, 0}},
    {"info", {0, 0, 0}},
    {"dbsize", {0, 0, 0}},
    {"time", {0, 0, 0}},
};

std::string to_lower(const butil::StringPiece& s) {
	std::string result(s.data(), s.size());
	std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
	return result;
}

// Decode slot range from region's start_key/end_key
// Returns true if this region covers the given slot
bool region_covers_slot(const pb::RegionInfo& region_info, uint16_t slot) {
	// Get the suffix part of start_key/end_key (after region_id+index_id prefix)
	// In our design, the suffix starts with slot (big-endian uint16)

	const std::string& start_key = region_info.start_key();
	const std::string& end_key = region_info.end_key();

	// Empty start_key means start from slot 0
	uint16_t start_slot = 0;
	if (!start_key.empty() && start_key.size() >= 2) {
		uint16_t slot_be;
		memcpy(&slot_be, start_key.data(), sizeof(slot_be));
		start_slot = to_endian_u16(slot_be);
	}

	// Empty end_key means extend to slot 16383
	uint16_t end_slot = 16384; // exclusive
	if (!end_key.empty() && end_key.size() >= 2) {
		uint16_t slot_be;
		memcpy(&slot_be, end_key.data(), sizeof(slot_be));
		end_slot = to_endian_u16(slot_be);
	}

	return slot >= start_slot && slot < end_slot;
}

} // namespace

int RedisRouter::init(const std::string& db_name, const std::string& table_name) {
	if (_initialized) {
		return 0;
	}

	// Resolve Redis table/index id from schema if available.
	// If schema isn't ready yet, keep it dynamic and resolve by region at route time.
	_redis_table_id = 0;
	_redis_index_id = 0;
	int64_t table_id = 0;
	const std::string full_table_name = db_name + "." + table_name;
	if (SchemaFactory::get_instance()->get_table_id(full_table_name, table_id) == 0 && table_id > 0) {
		_redis_table_id = table_id;
		_redis_index_id = table_id;
	}

	DB_NOTICE("Redis router initialized: table_id=%ld, index_id=%ld", _redis_table_id, _redis_index_id);
	_initialized = true;
	return 0;
}

uint16_t RedisRouter::calc_slot(const std::string& key) {
	return redis_slot(key);
}

std::string RedisRouter::extract_hash_tag(const std::string& key) {
	auto left = key.find('{');
	if (left == std::string::npos) {
		return key;
	}
	auto right = key.find('}', left + 1);
	if (right != std::string::npos && right > left + 1) {
		return key.substr(left + 1, right - left - 1);
	}
	return key;
}

const RedisKeyPattern* RedisRouter::get_key_pattern(const butil::StringPiece& cmd) {
	std::string cmd_lower = to_lower(cmd);
	auto it = kKeyPatterns.find(cmd_lower);
	if (it != kKeyPatterns.end()) {
		return &it->second;
	}
	return nullptr;
}

std::vector<std::string> RedisRouter::extract_keys(const std::vector<butil::StringPiece>& args) {
	std::vector<std::string> keys;

	if (args.empty()) {
		return keys;
	}

	const RedisKeyPattern* pattern = get_key_pattern(args[0]);
	if (pattern == nullptr || pattern->first_key_index == 0) {
		return keys; // No keys for this command
	}

	int first = pattern->first_key_index;
	int last = pattern->last_key_index;
	int step = pattern->key_step;

	if (last == -1) {
		last = static_cast<int>(args.size()) - 1;
	}

	// Validate indices
	if (first < 1 || first > static_cast<int>(args.size()) - 1) {
		return keys;
	}
	if (last > static_cast<int>(args.size()) - 1) {
		last = static_cast<int>(args.size()) - 1;
	}

	for (int i = first; i <= last; i += step) {
		keys.emplace_back(args[i].data(), args[i].size());
	}

	return keys;
}

int32_t RedisRouter::check_same_slot(const std::vector<std::string>& keys) {
	if (keys.empty()) {
		return 0; // No keys, default to slot 0
	}

	uint16_t first_slot = calc_slot(keys[0]);
	for (size_t i = 1; i < keys.size(); ++i) {
		if (calc_slot(keys[i]) != first_slot) {
			return -1; // CROSSSLOT
		}
	}
	return static_cast<int32_t>(first_slot);
}

RedisRouteResult RedisRouter::route(const std::vector<butil::StringPiece>& args) {
	RedisRouteResult result;

	if (args.empty()) {
		result.status = RedisRouteResult::INVALID_CMD;
		result.error_msg = "ERR empty command";
		return result;
	}

	// Extract keys
	std::vector<std::string> keys = extract_keys(args);

	// Commands with no keys are always OK
	if (keys.empty()) {
		result.status = RedisRouteResult::OK;
		result.slot = 0;
		return result;
	}

	// Check for CROSSSLOT
	int32_t slot = check_same_slot(keys);
	if (slot < 0) {
		result.status = RedisRouteResult::CROSSSLOT;
		result.error_msg = build_crossslot_error();
		return result;
	}

	result.slot = static_cast<uint16_t>(slot);

	// Find region for this slot
	int64_t region_id = 0;
	SmartRegion region = find_region_by_slot(result.slot, &region_id);
	if (region == nullptr) {
		result.status = RedisRouteResult::CLUSTERDOWN;
		result.error_msg = build_clusterdown_error();
		return result;
	}

	result.region_id = region_id;
	result.region = region;
	result.table_id = region->get_table_id();
	result.index_id = region->get_table_id();
	if (result.table_id <= 0 || result.index_id <= 0) {
		result.status = RedisRouteResult::CLUSTERDOWN;
		result.error_msg = build_clusterdown_error();
		return result;
	}

	// Check if we're the leader
	// For now, we'll let the command handler deal with leader checks
	// This allows read-your-writes on leader and MOVED on followers

	result.status = RedisRouteResult::OK;
	return result;
}

SmartRegion RedisRouter::find_region_by_slot(uint16_t slot, int64_t* out_region_id) {
	Store* store = Store::get_instance();
	if (store == nullptr) {
		return nullptr;
	}

	SmartRegion found_region = nullptr;
	int64_t found_region_id = 0;

	store->traverse_region_map([&](const SmartRegion& region) {
		if (found_region != nullptr) {
			return; // Already found
		}

		// In bootstrap mode (no specific redis table), accept any region
		// In normal mode, check table_id matches
		if (_redis_table_id > 0 && region->get_table_id() != _redis_table_id) {
			return;
		}

		// Check if this region covers the slot
		const pb::RegionInfo& region_info = region->region_info();
		if (region_covers_slot(region_info, slot)) {
			found_region = region;
			found_region_id = region->get_region_id();
			if (_redis_table_id == 0 && region->get_table_id() > 0) {
				_redis_table_id = region->get_table_id();
				_redis_index_id = _redis_table_id;
			}
		}
	});

	if (out_region_id != nullptr) {
		*out_region_id = found_region_id;
	}
	return found_region;
}

std::string RedisRouter::build_moved_error(uint16_t slot, const std::string& addr) {
	return "MOVED " + std::to_string(slot) + " " + addr;
}

std::string RedisRouter::build_crossslot_error() {
	return "CROSSSLOT Keys in request don't hash to the same slot";
}

std::string RedisRouter::build_clusterdown_error() {
	return "CLUSTERDOWN Hash slot not served";
}

} // namespace neokv
