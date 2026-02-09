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

// Redis metadata encoding layer for NeoKV.
// Adapted from Apache Kvrocks (ASF 2.0) redis_metadata.h/.cc,
// re-encoded to use NeoKV's region-based key prefix.

#pragma once

#include <cstdint>
#include <string>
#include <atomic>
#include <chrono>

namespace neokv {

// ============================================================================
// Redis type enum (stored in flags byte of metadata value)
// ============================================================================
enum RedisType : uint8_t {
	kRedisNone = 0,
	kRedisString = 1,
	kRedisHash = 2,
	kRedisList = 3,
	kRedisSet = 4,
	kRedisZSet = 5,
};

inline const char* redis_type_name(RedisType type) {
	switch (type) {
	case kRedisNone:
		return "none";
	case kRedisString:
		return "string";
	case kRedisHash:
		return "hash";
	case kRedisList:
		return "list";
	case kRedisSet:
		return "set";
	case kRedisZSet:
		return "zset";
	default:
		return "unknown";
	}
}

// ============================================================================
// Flags byte layout:
//   [1-bit: always-64bit] [3-bit: reserved] [4-bit: RedisType]
// ============================================================================
constexpr uint8_t METADATA_64BIT_ENCODING_MASK = 0x80;
constexpr uint8_t METADATA_TYPE_MASK = 0x0f;

// ============================================================================
// Metadata key encoding (stored in redis_metadata CF)
//
// Key format:
//   [region_id:8][index_id:8][slot:2][user_key_len:4][user_key]
//
// Same as RedisCodec::encode_key() but WITHOUT the trailing type_tag byte,
// because the type is stored in the value's flags byte.
// ============================================================================
class RedisMetadataKey {
public:
	// Encode a metadata key
	static std::string encode(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key);

	// Decode a metadata key
	static bool decode(const std::string& rocks_key, int64_t* region_id, int64_t* index_id, uint16_t* slot,
	                   std::string* user_key);
};

// ============================================================================
// Metadata value encoding
//
// String (single-KV, inline):
//   [flags:1][expire_ms:8][inline_value...]
//
// Compound types (Hash/List/Set/ZSet):
//   [flags:1][expire_ms:8][version:8][size:8]
//
// List extends compound with:
//   [flags:1][expire_ms:8][version:8][size:8][head:8][tail:8]
// ============================================================================
class RedisMetadata {
public:
	uint8_t flags;    // type + 64bit indicator
	uint64_t expire;  // absolute expiration in milliseconds, 0 = no expiration
	uint64_t version; // version for lazy deletion (compound types only)
	uint64_t size;    // element count (compound types only)

	// Construct metadata for a given type.
	// For compound types, generate_version=true creates a new version.
	// For strings, version/size are unused.
	explicit RedisMetadata(RedisType type, bool generate_version = true);

	// Type accessors
	RedisType type() const {
		return static_cast<RedisType>(flags & METADATA_TYPE_MASK);
	}
	bool is_64bit_encoded() const {
		return flags & METADATA_64BIT_ENCODING_MASK;
	}

	// Single-KV types store their value inline in metadata (no subkeys).
	// Currently only kRedisString.
	bool is_single_kv_type() const {
		return type() == kRedisString;
	}

	// Encode metadata (without inline value for strings — caller appends that)
	void encode(std::string* dst) const;

	// Decode metadata from a raw value slice.
	// On success, *input is advanced past the metadata header.
	// For strings, the remaining bytes in *input are the inline value.
	bool decode(const char** input, size_t* remaining);

	// Convenience: decode from a string
	bool decode(const std::string& raw_value);

	// Expiration helpers
	bool is_expired() const;
	bool is_expired(int64_t now_ms) const;
	int64_t ttl_ms() const;  // returns -1 if no expire, -2 if expired, else remaining ms
	int64_t ttl_sec() const; // returns -1 if no expire, -2 if expired, else remaining sec

	// Check if this metadata represents a "dead" key (expired or empty compound)
	bool is_dead() const;

	// Version generation (call once at startup)
	static void init_version_counter();

	// Get current time in milliseconds
	static int64_t current_time_ms();

	// Get current time in microseconds
	static int64_t current_time_us();

private:
	static uint64_t generate_version();
	static std::atomic<uint64_t> version_counter_;
};

// ============================================================================
// List metadata extends RedisMetadata with head/tail pointers
// ============================================================================
class RedisListMetadata : public RedisMetadata {
public:
	uint64_t head;
	uint64_t tail;

	explicit RedisListMetadata(bool generate_version = true);

	void encode(std::string* dst) const;
	bool decode(const char** input, size_t* remaining);
	bool decode(const std::string& raw_value);
};

// ============================================================================
// Subkey (InternalKey) encoding — stored in data CF
//
// Key format:
//   [region_id:8][index_id:8][slot:2][user_key_len:4][user_key][version:8][sub_key]
//
// The prefix (up to user_key) is the same as the metadata key.
// version must match the metadata's current version.
// sub_key meaning depends on type:
//   Hash:  field name
//   Set:   member
//   List:  big-endian uint64 index
//   ZSet:  member
// ============================================================================
class RedisSubkeyKey {
public:
	// Encode a subkey
	static std::string encode(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
	                          uint64_t version, const std::string& sub_key);

	// Build a prefix for iterating all subkeys of a given key+version
	static std::string encode_prefix(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
	                                 uint64_t version);

	// Decode a subkey. Returns true on success.
	// sub_key will contain everything after the version.
	static bool decode(const std::string& rocks_key, int64_t* region_id, int64_t* index_id, uint16_t* slot,
	                   std::string* user_key, uint64_t* version, std::string* sub_key);
};

// ============================================================================
// ZSet score key encoding — stored in redis_zset_score CF
//
// Key format:
//   [region_id:8][index_id:8][slot:2][user_key_len:4][user_key][version:8][score:8][member]
//
// score is encoded as IEEE 754 double in a byte-order-preserving format.
// ============================================================================
class RedisZSetScoreKey {
public:
	// Encode a score key
	static std::string encode(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
	                          uint64_t version, double score, const std::string& member);

	// Build prefix for iterating all scores of a given key+version
	static std::string encode_prefix(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
	                                 uint64_t version);

	// Encode a double to 8 bytes that preserve sort order
	static void encode_double(double val, std::string* dst);

	// Decode 8 bytes back to double
	static double decode_double(const char* data);
};

} // namespace neokv
