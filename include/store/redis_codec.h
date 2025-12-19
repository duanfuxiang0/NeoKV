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

namespace baikaldb {

// Redis type tags (stored as last byte of key suffix)
enum RedisTypeTag : uint8_t {
    REDIS_STRING = 'S',
    REDIS_HASH   = 'H',
    REDIS_LIST   = 'L',
    REDIS_SET    = 'E',  // 'E' for sEt
    REDIS_ZSET   = 'Z',
};

// Constant for no expiration
constexpr int64_t REDIS_NO_EXPIRE = 0;

// Redis key/value codec
// 
// RocksDB Key format:
//   [region_id:8][index_id:8][slot:2][user_key_len:4][user_key][type_tag:1]
//   - region_id/index_id: use MutTableKey.append_i64() encoding (mem-comparable)
//   - slot: big-endian uint16
//   - user_key_len: big-endian uint32
//   - user_key: raw bytes
//   - type_tag: single byte (e.g. 'S' for string)
//
// RocksDB Value format:
//   [expire_at_ms:8][payload]
//   - expire_at_ms: big-endian int64, 0 means no expiration
//   - payload: raw value bytes

class RedisCodec {
public:
    // Encode a Redis key to RocksDB key format
    // Returns the encoded key string
    static std::string encode_key(int64_t region_id,
                                  int64_t index_id,
                                  uint16_t slot,
                                  const std::string& user_key,
                                  RedisTypeTag type_tag = REDIS_STRING);

    // Decode a RocksDB key back to its components
    // Returns true on success, false if the key format is invalid
    static bool decode_key(const std::string& rocks_key,
                           int64_t* region_id,
                           int64_t* index_id,
                           uint16_t* slot,
                           std::string* user_key,
                           RedisTypeTag* type_tag);

    // Encode a Redis value with TTL
    // expire_at_ms: absolute expiration time in milliseconds since epoch, 0 = no expiration
    static std::string encode_value(int64_t expire_at_ms, const std::string& payload);

    // Decode a RocksDB value
    // Returns true on success
    static bool decode_value(const std::string& rocks_value,
                             int64_t* expire_at_ms,
                             std::string* payload);

    // Check if a value has expired
    // current_ms: current time in milliseconds since epoch
    static bool is_expired(int64_t expire_at_ms, int64_t current_ms);

    // Get current time in milliseconds since epoch
    static int64_t current_time_ms();

    // Compute TTL in seconds from expire_at_ms
    // Returns -2 if key doesn't exist (caller should check)
    // Returns -1 if no expiration
    // Returns remaining seconds otherwise
    static int64_t compute_ttl_seconds(int64_t expire_at_ms);

    // Build RocksDB key prefix for a region (for iteration)
    static std::string build_region_prefix(int64_t region_id, int64_t index_id);

    // Build RocksDB key prefix for a slot within a region (for iteration)
    static std::string build_slot_prefix(int64_t region_id, int64_t index_id, uint16_t slot);
};

} // namespace baikaldb
