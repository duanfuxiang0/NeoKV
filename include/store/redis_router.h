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
#include <string>
#include <vector>
#include <memory>
#include <functional>
#ifdef BAIDU_INTERNAL
#include <butil/strings/string_piece.h>
#else
#include <butil/strings/string_piece.h>
#endif

namespace baikaldb {

class Region;
using SmartRegion = std::shared_ptr<Region>;

// Key extraction pattern for Redis commands
struct RedisKeyPattern {
    int first_key_index;     // Index of first key in args (1-based, 0 = no keys)
    int last_key_index;      // Index of last key (-1 = all remaining args are keys)
    int key_step;            // Step between keys (1 = consecutive, 2 = key-value pairs)
};

// Result of routing a Redis command
struct RedisRouteResult {
    enum Status {
        OK,
        CROSSSLOT,       // Keys hash to different slots
        CLUSTERDOWN,     // No region serves this slot
        NOT_LEADER,      // Current node is not leader for this slot
        INVALID_CMD,     // Invalid command or arguments
    };

    Status status = OK;
    uint16_t slot = 0;
    int64_t region_id = 0;
    SmartRegion region;
    std::string leader_addr;  // For MOVED response
    std::string error_msg;
};

// Redis router for slot calculation and region routing
class RedisRouter {
public:
    // Get the singleton instance
    static RedisRouter* get_instance() {
        static RedisRouter instance;
        return &instance;
    }

    // Initialize the router (call after SchemaFactory is ready)
    // table_name: fully qualified table name (e.g., "__redis__.kv")
    int init(const std::string& db_name = "__redis__",
             const std::string& table_name = "kv");

    // Get the Redis table ID and index ID
    int64_t get_redis_table_id() const { return _redis_table_id; }
    int64_t get_redis_index_id() const { return _redis_index_id; }
    bool is_initialized() const { return _initialized; }

    // Calculate slot for a key (with {tag} support)
    // This is the same as redis_slot() but provided here for convenience
    static uint16_t calc_slot(const std::string& key);

    // Extract hash tag from key if present
    // Returns the original key if no valid tag found
    static std::string extract_hash_tag(const std::string& key);

    // Get key extraction pattern for a command
    // Returns nullptr if command has no keys or is unknown
    static const RedisKeyPattern* get_key_pattern(const butil::StringPiece& cmd);

    // Extract keys from command arguments
    // args[0] is the command name
    // Returns empty vector if no keys or invalid command
    static std::vector<std::string> extract_keys(
        const std::vector<butil::StringPiece>& args);

    // Check if all keys hash to the same slot
    // Returns the slot if all keys are in the same slot, or -1 if CROSSSLOT
    static int32_t check_same_slot(const std::vector<std::string>& keys);

    // Route a command to a region
    // args[0] is the command name
    // Returns routing result with status and region info
    RedisRouteResult route(const std::vector<butil::StringPiece>& args);

    // Find region by slot
    // Returns nullptr if no region serves this slot
    SmartRegion find_region_by_slot(uint16_t slot, int64_t* region_id = nullptr);

    // Build MOVED error response string
    static std::string build_moved_error(uint16_t slot, const std::string& addr);

    // Build CROSSSLOT error response string
    static std::string build_crossslot_error();

    // Build CLUSTERDOWN error response string
    static std::string build_clusterdown_error();

private:
    RedisRouter() = default;
    ~RedisRouter() = default;
    RedisRouter(const RedisRouter&) = delete;
    RedisRouter& operator=(const RedisRouter&) = delete;

    bool _initialized = false;
    int64_t _redis_table_id = 0;
    int64_t _redis_index_id = 0;
};

} // namespace baikaldb
