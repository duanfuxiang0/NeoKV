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
#include "redis_common.h"
#include "redis_service.h"
#include "store.h"
#include "region.h"
#include "common.h"
#include "schema_factory.h"
#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <arpa/inet.h>

// Implementation of is_redis_table() declared in redis_common.h.
// Placed here to avoid circular header dependencies (redis_common.h ↔ redis_router.h).
namespace neokv {
bool is_redis_table(int64_t table_id) {
	if (table_id <= 0) {
		return false;
	}
	int64_t redis_tid = RedisRouter::get_instance()->get_redis_table_id();
	if (redis_tid > 0) {
		return table_id == redis_tid;
	}
	// Router not yet resolved — fall back to accepting any positive table_id
	// (safe in pure-Redis deployments; mixed deployments should ensure router is initialized)
	return true;
}
} // namespace neokv

namespace {
// Big-endian conversion for slot encoding (self-inverse on both LE and BE hosts)
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
    // ---- Implemented commands (Phase 1) ----
    {"get", {1, 1, 1}},
    {"set", {1, 1, 1}},
    {"mget", {1, -1, 1}},
    {"mset", {1, -1, 2}}, // key value key value ...
    {"del", {1, -1, 1}},
    {"expire", {1, 1, 1}},
    {"ttl", {1, 1, 1}},

    // ---- Routing-only (key pattern registered for CROSSSLOT validation, ----
    // ---- but command handler not yet implemented)                        ----
    {"setnx", {1, 1, 1}},       // TODO: implement handler
    {"setex", {1, 1, 1}},       // TODO: implement handler
    {"psetex", {1, 1, 1}},      // TODO: implement handler
    {"msetnx", {1, -1, 2}},     // TODO: implement handler
    {"unlink", {1, -1, 1}},     // TODO: implement handler
    {"exists", {1, -1, 1}},     // TODO: implement handler
    {"expireat", {1, 1, 1}},    // TODO: implement handler
    {"pexpire", {1, 1, 1}},     // TODO: implement handler
    {"pexpireat", {1, 1, 1}},   // TODO: implement handler
    {"pttl", {1, 1, 1}},        // TODO: implement handler
    {"persist", {1, 1, 1}},     // TODO: implement handler
    {"type", {1, 1, 1}},        // TODO: implement handler
    {"getset", {1, 1, 1}},      // TODO: implement handler
    {"append", {1, 1, 1}},      // TODO: implement handler
    {"strlen", {1, 1, 1}},      // TODO: implement handler
    {"incr", {1, 1, 1}},        // TODO: implement handler
    {"decr", {1, 1, 1}},        // TODO: implement handler
    {"incrby", {1, 1, 1}},      // TODO: implement handler
    {"decrby", {1, 1, 1}},      // TODO: implement handler
    {"incrbyfloat", {1, 1, 1}}, // TODO: implement handler
    {"getrange", {1, 1, 1}},    // GETRANGE key start end
    {"setrange", {1, 1, 1}},    // SETRANGE key offset value
    {"getdel", {1, 1, 1}},      // GETDEL key
    {"getex", {1, 1, 1}},       // GETEX key [EX|PX|EXAT|PXAT|PERSIST]

    // ---- Hash commands ----
    {"hset", {1, 1, 1}},         // HSET key field value [field value ...]
    {"hget", {1, 1, 1}},         // HGET key field
    {"hdel", {1, 1, 1}},         // HDEL key field [field ...]
    {"hmset", {1, 1, 1}},        // HMSET key field value [field value ...]
    {"hmget", {1, 1, 1}},        // HMGET key field [field ...]
    {"hgetall", {1, 1, 1}},      // HGETALL key
    {"hkeys", {1, 1, 1}},        // HKEYS key
    {"hvals", {1, 1, 1}},        // HVALS key
    {"hlen", {1, 1, 1}},         // HLEN key
    {"hexists", {1, 1, 1}},      // HEXISTS key field
    {"hsetnx", {1, 1, 1}},       // HSETNX key field value
    {"hincrby", {1, 1, 1}},      // HINCRBY key field increment
    {"hincrbyfloat", {1, 1, 1}}, // HINCRBYFLOAT key field increment
    {"hrandfield", {1, 1, 1}},   // HRANDFIELD key [count [WITHVALUES]]
    {"hscan", {1, 1, 1}},        // HSCAN key cursor [MATCH pattern] [COUNT count]

    // ---- Set commands ----
    {"sadd", {1, 1, 1}},         // SADD key member [member ...]
    {"srem", {1, 1, 1}},         // SREM key member [member ...]
    {"smembers", {1, 1, 1}},     // SMEMBERS key
    {"sismember", {1, 1, 1}},    // SISMEMBER key member
    {"scard", {1, 1, 1}},        // SCARD key
    {"srandmember", {1, 1, 1}},  // SRANDMEMBER key [count]
    {"spop", {1, 1, 1}},         // SPOP key [count]
    {"smismember", {1, 1, 1}},   // SMISMEMBER key member [member ...]
    {"sscan", {1, 1, 1}},        // SSCAN key cursor [MATCH pattern] [COUNT count]
    {"sinter", {1, -1, 1}},      // SINTER key [key ...] — multi-key, all args are keys
    {"sunion", {1, -1, 1}},      // SUNION key [key ...] — multi-key, all args are keys
    {"sdiff", {1, -1, 1}},       // SDIFF key [key ...] — multi-key, all args are keys
    {"sintercard", {0, 0, 0}},   // SINTERCARD numkeys key [key ...] [LIMIT limit]
    {"smove", {1, 2, 1}},        // SMOVE source destination member
    {"sinterstore", {1, -1, 1}}, // SINTERSTORE destination key [key ...]
    {"sunionstore", {1, -1, 1}}, // SUNIONSTORE destination key [key ...]
    {"sdiffstore", {1, -1, 1}},  // SDIFFSTORE destination key [key ...]

    // ---- Sorted Set commands ----
    {"zadd", {1, 1, 1}},             // ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
    {"zrem", {1, 1, 1}},             // ZREM key member [member ...]
    {"zscore", {1, 1, 1}},           // ZSCORE key member
    {"zrank", {1, 1, 1}},            // ZRANK key member
    {"zrevrank", {1, 1, 1}},         // ZREVRANK key member
    {"zcard", {1, 1, 1}},            // ZCARD key
    {"zcount", {1, 1, 1}},           // ZCOUNT key min max
    {"zrange", {1, 1, 1}},           // ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    {"zrangebyscore", {1, 1, 1}},    // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    {"zrevrange", {1, 1, 1}},        // ZREVRANGE key start stop [WITHSCORES]
    {"zrevrangebyscore", {1, 1, 1}}, // ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    {"zincrby", {1, 1, 1}},          // ZINCRBY key increment member
    {"zrangebylex", {1, 1, 1}},      // ZRANGEBYLEX key min max [LIMIT offset count]
    {"zpopmin", {1, 1, 1}},          // ZPOPMIN key [count]
    {"zpopmax", {1, 1, 1}},          // ZPOPMAX key [count]
    {"zscan", {1, 1, 1}},            // ZSCAN key cursor [MATCH pattern] [COUNT count]
    {"zlexcount", {1, 1, 1}},        // ZLEXCOUNT key min max

    // ---- List commands ----
    {"lpush", {1, 1, 1}},   // LPUSH key element [element ...]
    {"rpush", {1, 1, 1}},   // RPUSH key element [element ...]
    {"lpop", {1, 1, 1}},    // LPOP key [count]
    {"rpop", {1, 1, 1}},    // RPOP key [count]
    {"llen", {1, 1, 1}},    // LLEN key
    {"lindex", {1, 1, 1}},  // LINDEX key index
    {"lrange", {1, 1, 1}},  // LRANGE key start stop
    {"lpos", {1, 1, 1}},    // LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
    {"lset", {1, 1, 1}},    // LSET key index element
    {"linsert", {1, 1, 1}}, // LINSERT key BEFORE|AFTER pivot element
    {"lrem", {1, 1, 1}},    // LREM key count element
    {"ltrim", {1, 1, 1}},   // LTRIM key start stop
    {"lmove", {1, 2, 1}},   // LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    {"lmpop", {0, 0, 0}},   // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]

    // ---- Commands with no keys (handled specially) ----
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
// Populates [out_start, out_end_exclusive) slot range.
void decode_slot_range_from_region(const pb::RegionInfo& region_info, uint16_t* out_start,
                                   uint16_t* out_end_exclusive) {
	const std::string& start_key = region_info.start_key();
	const std::string& end_key = region_info.end_key();

	*out_start = 0;
	if (!start_key.empty() && start_key.size() >= 2) {
		uint16_t slot_be;
		memcpy(&slot_be, start_key.data(), sizeof(slot_be));
		*out_start = to_endian_u16(slot_be);
	}

	*out_end_exclusive = 16384; // past-the-end
	if (!end_key.empty() && end_key.size() >= 2) {
		uint16_t slot_be;
		memcpy(&slot_be, end_key.data(), sizeof(slot_be));
		*out_end_exclusive = to_endian_u16(slot_be);
	}
}

// Check if a region covers a given slot
bool region_covers_slot(const pb::RegionInfo& region_info, uint16_t slot) {
	uint16_t start = 0, end_exclusive = 16384;
	decode_slot_range_from_region(region_info, &start, &end_exclusive);
	return slot >= start && slot < end_exclusive;
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

// ============================================================================
// Slot table management
// ============================================================================

void RedisRouter::rebuild_slot_table() {
	Store* store = Store::get_instance();
	if (store == nullptr) {
		return;
	}

	auto new_table = std::make_shared<SlotTable>();

	store->traverse_region_map([&](const SmartRegion& region) {
		if (region == nullptr) {
			return;
		}
		if (_redis_table_id > 0 && region->get_table_id() != _redis_table_id) {
			return;
		}

		const pb::RegionInfo& info = region->region_info();
		uint16_t start = 0, end_exclusive = 16384;
		decode_slot_range_from_region(info, &start, &end_exclusive);

		int64_t rid = region->get_region_id();
		for (uint32_t s = start; s < end_exclusive && s < SLOT_TABLE_SIZE; ++s) {
			new_table->slots[s].region = region;
			new_table->slots[s].region_id = rid;
		}

		// Auto-detect redis table_id if not yet resolved
		if (_redis_table_id == 0 && region->get_table_id() > 0) {
			_redis_table_id = region->get_table_id();
			_redis_index_id = _redis_table_id;
		}
	});

	{
		std::lock_guard<std::mutex> lock(_slot_table_mutex);
		_slot_table = std::move(new_table);
	}
}

// ============================================================================
// Slot calculation & key extraction
// ============================================================================

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

// ============================================================================
// Routing
// ============================================================================

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

	// Leader check is deferred to command handlers (allows follower read)
	result.status = RedisRouteResult::OK;
	return result;
}

SmartRegion RedisRouter::find_region_by_slot(uint16_t slot, int64_t* out_region_id) {
	// Fast path: O(1) lookup from slot table
	SlotTablePtr table;
	{
		std::lock_guard<std::mutex> lock(_slot_table_mutex);
		table = _slot_table;
	}

	if (table != nullptr && slot < SLOT_TABLE_SIZE) {
		const auto& entry = table->slots[slot];
		if (entry.region != nullptr) {
			// Verify the cached region is still in the Store's region map.
			// Regions can be removed (erase_region) or replaced at any time.
			Store* store = Store::get_instance();
			if (store != nullptr) {
				SmartRegion current = store->get_region(entry.region_id);
				if (current != nullptr && current == entry.region) {
					if (out_region_id != nullptr) {
						*out_region_id = entry.region_id;
					}
					return entry.region;
				}
			}
			// Cached entry is stale — fall through to slow path which will rebuild
		}
	}

	// Slow path: linear scan (startup / slot table stale / region removed)
	return find_region_by_slot_slow(slot, out_region_id);
}

SmartRegion RedisRouter::find_region_by_slot_slow(uint16_t slot, int64_t* out_region_id) {
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

	// If we found something via slow path, trigger a rebuild so future lookups are fast
	if (found_region != nullptr) {
		rebuild_slot_table();
	}

	return found_region;
}

// ============================================================================
// Error message builders
// ============================================================================

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
