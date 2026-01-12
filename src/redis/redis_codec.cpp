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

#include "redis_codec.h"
#include "mut_table_key.h"
#include "key_encoder.h"
#include <chrono>
#include <cstring>

namespace neokv {

std::string RedisCodec::encode_key(int64_t region_id,
                                   int64_t index_id,
                                   uint16_t slot,
                                   const std::string& user_key,
                                   RedisTypeTag type_tag) {
    // Use MutTableKey for the region_id and index_id prefix (mem-comparable encoding)
    MutTableKey prefix;
    prefix.append_i64(region_id);
    prefix.append_i64(index_id);

    std::string result = prefix.data();

    // Append slot (big-endian uint16)
    uint16_t slot_be = KeyEncoder::to_endian_u16(slot);
    result.append(reinterpret_cast<const char*>(&slot_be), sizeof(slot_be));

    // Append user_key_len (big-endian uint32)
    uint32_t key_len = static_cast<uint32_t>(user_key.size());
    uint32_t key_len_be = KeyEncoder::to_endian_u32(key_len);
    result.append(reinterpret_cast<const char*>(&key_len_be), sizeof(key_len_be));

    // Append user_key
    result.append(user_key);

    // Append type_tag
    result.push_back(static_cast<char>(type_tag));

    return result;
}

bool RedisCodec::decode_key(const std::string& rocks_key,
                            int64_t* region_id,
                            int64_t* index_id,
                            uint16_t* slot,
                            std::string* user_key,
                            RedisTypeTag* type_tag) {
    // Minimum size: 8 (region_id) + 8 (index_id) + 2 (slot) + 4 (key_len) + 1 (type_tag) = 23
    if (rocks_key.size() < 23) {
        return false;
    }

    const char* ptr = rocks_key.data();
    size_t remaining = rocks_key.size();

    // Decode region_id (mem-comparable encoding)
    if (region_id) {
        uint64_t encoded;
        std::memcpy(&encoded, ptr, sizeof(encoded));
        *region_id = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(encoded));
    }
    ptr += 8;
    remaining -= 8;

    // Decode index_id (mem-comparable encoding)
    if (index_id) {
        uint64_t encoded;
        std::memcpy(&encoded, ptr, sizeof(encoded));
        *index_id = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(encoded));
    }
    ptr += 8;
    remaining -= 8;

    // Decode slot (big-endian uint16)
    if (remaining < 2) return false;
    if (slot) {
        uint16_t slot_be;
        std::memcpy(&slot_be, ptr, sizeof(slot_be));
        *slot = KeyEncoder::to_endian_u16(slot_be);
    }
    ptr += 2;
    remaining -= 2;

    // Decode user_key_len (big-endian uint32)
    if (remaining < 4) return false;
    uint32_t key_len_be;
    std::memcpy(&key_len_be, ptr, sizeof(key_len_be));
    uint32_t key_len = KeyEncoder::to_endian_u32(key_len_be);
    ptr += 4;
    remaining -= 4;

    // Check remaining size: key_len + 1 (type_tag)
    if (remaining < key_len + 1) {
        return false;
    }

    // Decode user_key
    if (user_key) {
        user_key->assign(ptr, key_len);
    }
    ptr += key_len;

    // Decode type_tag
    if (type_tag) {
        *type_tag = static_cast<RedisTypeTag>(*ptr);
    }

    return true;
}

std::string RedisCodec::encode_value(int64_t expire_at_ms, const std::string& payload) {
    std::string result;
    result.reserve(8 + payload.size());

    // Encode expire_at_ms as big-endian int64
    uint64_t expire_be = KeyEncoder::to_endian_u64(static_cast<uint64_t>(expire_at_ms));
    result.append(reinterpret_cast<const char*>(&expire_be), sizeof(expire_be));

    // Append payload
    result.append(payload);

    return result;
}

bool RedisCodec::decode_value(const std::string& rocks_value,
                              int64_t* expire_at_ms,
                              std::string* payload) {
    if (rocks_value.size() < 8) {
        return false;
    }

    // Decode expire_at_ms
    if (expire_at_ms) {
        uint64_t expire_be;
        std::memcpy(&expire_be, rocks_value.data(), sizeof(expire_be));
        *expire_at_ms = static_cast<int64_t>(KeyEncoder::to_endian_u64(expire_be));
    }

    // Extract payload
    if (payload) {
        payload->assign(rocks_value.data() + 8, rocks_value.size() - 8);
    }

    return true;
}

bool RedisCodec::is_expired(int64_t expire_at_ms, int64_t current_ms) {
    if (expire_at_ms == REDIS_NO_EXPIRE) {
        return false;
    }
    return current_ms >= expire_at_ms;
}

int64_t RedisCodec::current_time_ms() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch());
    return ms.count();
}

int64_t RedisCodec::compute_ttl_seconds(int64_t expire_at_ms) {
    if (expire_at_ms == REDIS_NO_EXPIRE) {
        return -1;  // No expiration
    }
    int64_t now_ms = current_time_ms();
    if (now_ms >= expire_at_ms) {
        return 0;  // Already expired, but key exists
    }
    return (expire_at_ms - now_ms) / 1000;
}

std::string RedisCodec::build_region_prefix(int64_t region_id, int64_t index_id) {
    MutTableKey prefix;
    prefix.append_i64(region_id);
    prefix.append_i64(index_id);
    return prefix.data();
}

std::string RedisCodec::build_slot_prefix(int64_t region_id, int64_t index_id, uint16_t slot) {
    MutTableKey prefix;
    prefix.append_i64(region_id);
    prefix.append_i64(index_id);

    std::string result = prefix.data();

    // Append slot (big-endian uint16)
    uint16_t slot_be = KeyEncoder::to_endian_u16(slot);
    result.append(reinterpret_cast<const char*>(&slot_be), sizeof(slot_be));

    return result;
}

} // namespace neokv
