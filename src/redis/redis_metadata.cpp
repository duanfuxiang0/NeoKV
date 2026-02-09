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
// Adapted from Apache Kvrocks (ASF 2.0) redis_metadata.h/.cc.

#include "redis_metadata.h"
#include "mut_table_key.h"
#include "key_encoder.h"
#include <cstring>
#include <cstdlib>

namespace neokv {

// Version counter: 52-bit microsecond timestamp + 11-bit counter
static constexpr int kVersionCounterBits = 11;

std::atomic<uint64_t> RedisMetadata::version_counter_ {0};

// ============================================================================
// RedisMetadata
// ============================================================================

RedisMetadata::RedisMetadata(RedisType type, bool gen_version)
    : flags(METADATA_64BIT_ENCODING_MASK | (METADATA_TYPE_MASK & type)), expire(0), version(0), size(0) {
	// Generate version for compound types only
	if (!is_single_kv_type() && gen_version) {
		version = generate_version();
	}
}

// We need a static helper since the constructor can't call generate_version()
// before the object is fully constructed (is_single_kv_type() depends on flags).
// Actually, flags is set first in the initializer list, so is_single_kv_type() works.
// But let's use a separate static to be safe.
uint64_t RedisMetadata::generate_version() {
	int64_t timestamp = current_time_us();
	uint64_t counter = version_counter_.fetch_add(1);
	return (static_cast<uint64_t>(timestamp) << kVersionCounterBits) + (counter % (1 << kVersionCounterBits));
}

void RedisMetadata::init_version_counter() {
	// Use random initial counter to avoid conflicts when a follower is promoted
	version_counter_ = static_cast<uint64_t>(std::rand());
}

int64_t RedisMetadata::current_time_ms() {
	auto now = std::chrono::system_clock::now();
	return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
}

int64_t RedisMetadata::current_time_us() {
	auto now = std::chrono::system_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
}

void RedisMetadata::encode(std::string* dst) const {
	// flags: 1 byte
	dst->push_back(static_cast<char>(flags));

	// expire: 8 bytes big-endian (always 64-bit)
	uint64_t expire_be = KeyEncoder::host_to_be64(expire);
	dst->append(reinterpret_cast<const char*>(&expire_be), 8);

	// For compound types: version + size
	if (!is_single_kv_type()) {
		uint64_t version_be = KeyEncoder::host_to_be64(version);
		dst->append(reinterpret_cast<const char*>(&version_be), 8);

		uint64_t size_be = KeyEncoder::host_to_be64(size);
		dst->append(reinterpret_cast<const char*>(&size_be), 8);
	}
}

bool RedisMetadata::decode(const char** input, size_t* remaining) {
	// flags: 1 byte
	if (*remaining < 1)
		return false;
	flags = static_cast<uint8_t>(**input);
	(*input)++;
	(*remaining)--;

	// expire: 8 bytes
	if (*remaining < 8)
		return false;
	uint64_t expire_be;
	std::memcpy(&expire_be, *input, 8);
	expire = KeyEncoder::be_to_host64(expire_be);
	(*input) += 8;
	(*remaining) -= 8;

	// For compound types: version + size
	if (!is_single_kv_type()) {
		if (*remaining < 16)
			return false;

		uint64_t version_be;
		std::memcpy(&version_be, *input, 8);
		version = KeyEncoder::be_to_host64(version_be);
		(*input) += 8;
		(*remaining) -= 8;

		uint64_t size_be;
		std::memcpy(&size_be, *input, 8);
		size = KeyEncoder::be_to_host64(size_be);
		(*input) += 8;
		(*remaining) -= 8;
	}

	return true;
}

bool RedisMetadata::decode(const std::string& raw_value) {
	const char* ptr = raw_value.data();
	size_t remaining = raw_value.size();
	return decode(&ptr, &remaining);
}

bool RedisMetadata::is_expired() const {
	return is_expired(current_time_ms());
}

bool RedisMetadata::is_expired(int64_t now_ms) const {
	if (expire == 0)
		return false;
	return now_ms >= static_cast<int64_t>(expire);
}

int64_t RedisMetadata::ttl_ms() const {
	if (expire == 0)
		return -1;
	int64_t now = current_time_ms();
	if (now >= static_cast<int64_t>(expire))
		return -2;
	return static_cast<int64_t>(expire) - now;
}

int64_t RedisMetadata::ttl_sec() const {
	int64_t ms = ttl_ms();
	if (ms < 0)
		return ms; // -1 or -2
	return ms / 1000;
}

bool RedisMetadata::is_dead() const {
	if (is_expired())
		return true;
	// For compound types, size==0 means the key has been emptied
	if (!is_single_kv_type() && size == 0)
		return true;
	return false;
}

// ============================================================================
// RedisListMetadata
// ============================================================================

RedisListMetadata::RedisListMetadata(bool generate_version)
    : RedisMetadata(kRedisList, generate_version), head(UINT64_MAX / 2), tail(UINT64_MAX / 2) {
}

void RedisListMetadata::encode(std::string* dst) const {
	RedisMetadata::encode(dst);
	uint64_t head_be = KeyEncoder::host_to_be64(head);
	dst->append(reinterpret_cast<const char*>(&head_be), 8);
	uint64_t tail_be = KeyEncoder::host_to_be64(tail);
	dst->append(reinterpret_cast<const char*>(&tail_be), 8);
}

bool RedisListMetadata::decode(const char** input, size_t* remaining) {
	if (!RedisMetadata::decode(input, remaining))
		return false;
	if (*remaining < 16)
		return false;

	uint64_t head_be;
	std::memcpy(&head_be, *input, 8);
	head = KeyEncoder::be_to_host64(head_be);
	(*input) += 8;
	(*remaining) -= 8;

	uint64_t tail_be;
	std::memcpy(&tail_be, *input, 8);
	tail = KeyEncoder::be_to_host64(tail_be);
	(*input) += 8;
	(*remaining) -= 8;

	return true;
}

bool RedisListMetadata::decode(const std::string& raw_value) {
	const char* ptr = raw_value.data();
	size_t remaining = raw_value.size();
	return decode(&ptr, &remaining);
}

// ============================================================================
// RedisMetadataKey — metadata CF key encoding
// ============================================================================

std::string RedisMetadataKey::encode(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key) {
	// [region_id:8][index_id:8][slot:2][user_key_len:4][user_key]
	MutTableKey prefix;
	prefix.append_i64(region_id);
	prefix.append_i64(index_id);

	std::string result = prefix.data();

	// slot: big-endian uint16
	uint16_t slot_be = KeyEncoder::host_to_be16(slot);
	result.append(reinterpret_cast<const char*>(&slot_be), sizeof(slot_be));

	// user_key_len: big-endian uint32
	uint32_t key_len = static_cast<uint32_t>(user_key.size());
	uint32_t key_len_be = KeyEncoder::host_to_be32(key_len);
	result.append(reinterpret_cast<const char*>(&key_len_be), sizeof(key_len_be));

	// user_key
	result.append(user_key);

	return result;
}

bool RedisMetadataKey::decode(const std::string& rocks_key, int64_t* region_id, int64_t* index_id, uint16_t* slot,
                              std::string* user_key) {
	// Minimum: 8 + 8 + 2 + 4 = 22 bytes (empty user_key)
	if (rocks_key.size() < 22)
		return false;

	const char* ptr = rocks_key.data();

	// region_id
	if (region_id) {
		uint64_t encoded;
		std::memcpy(&encoded, ptr, 8);
		*region_id = KeyEncoder::decode_i64(KeyEncoder::be_to_host64(encoded));
	}
	ptr += 8;

	// index_id
	if (index_id) {
		uint64_t encoded;
		std::memcpy(&encoded, ptr, 8);
		*index_id = KeyEncoder::decode_i64(KeyEncoder::be_to_host64(encoded));
	}
	ptr += 8;

	// slot
	if (slot) {
		uint16_t slot_be;
		std::memcpy(&slot_be, ptr, 2);
		*slot = KeyEncoder::be_to_host16(slot_be);
	}
	ptr += 2;

	// user_key_len
	uint32_t key_len_be;
	std::memcpy(&key_len_be, ptr, 4);
	uint32_t key_len = KeyEncoder::be_to_host32(key_len_be);
	ptr += 4;

	// Verify remaining bytes
	size_t consumed = ptr - rocks_key.data();
	if (rocks_key.size() < consumed + key_len)
		return false;

	if (user_key) {
		user_key->assign(ptr, key_len);
	}

	return true;
}

// ============================================================================
// RedisSubkeyKey — data CF subkey encoding
// ============================================================================

std::string RedisSubkeyKey::encode(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                                   uint64_t version, const std::string& sub_key) {
	// [region_id:8][index_id:8][slot:2][user_key_len:4][user_key][version:8][sub_key]
	std::string result = RedisMetadataKey::encode(region_id, index_id, slot, user_key);

	// version: big-endian uint64
	uint64_t version_be = KeyEncoder::host_to_be64(version);
	result.append(reinterpret_cast<const char*>(&version_be), 8);

	// sub_key
	result.append(sub_key);

	return result;
}

std::string RedisSubkeyKey::encode_prefix(int64_t region_id, int64_t index_id, uint16_t slot,
                                          const std::string& user_key, uint64_t version) {
	std::string result = RedisMetadataKey::encode(region_id, index_id, slot, user_key);

	uint64_t version_be = KeyEncoder::host_to_be64(version);
	result.append(reinterpret_cast<const char*>(&version_be), 8);

	return result;
}

bool RedisSubkeyKey::decode(const std::string& rocks_key, int64_t* region_id, int64_t* index_id, uint16_t* slot,
                            std::string* user_key, uint64_t* version, std::string* sub_key) {
	// First decode the metadata key prefix
	std::string uk;
	if (!RedisMetadataKey::decode(rocks_key, region_id, index_id, slot, &uk)) {
		return false;
	}

	// Calculate where the metadata key prefix ends
	// 8 (region_id) + 8 (index_id) + 2 (slot) + 4 (key_len) + key_len
	size_t prefix_len = 8 + 8 + 2 + 4 + uk.size();

	// Need at least 8 more bytes for version
	if (rocks_key.size() < prefix_len + 8)
		return false;

	const char* ptr = rocks_key.data() + prefix_len;

	// version
	if (version) {
		uint64_t version_be;
		std::memcpy(&version_be, ptr, 8);
		*version = KeyEncoder::be_to_host64(version_be);
	}
	ptr += 8;

	// sub_key: everything remaining
	if (user_key) {
		*user_key = uk;
	}
	if (sub_key) {
		size_t sub_key_offset = prefix_len + 8;
		sub_key->assign(rocks_key.data() + sub_key_offset, rocks_key.size() - sub_key_offset);
	}

	return true;
}

// ============================================================================
// RedisZSetScoreKey — redis_zset_score CF encoding
// ============================================================================

void RedisZSetScoreKey::encode_double(double val, std::string* dst) {
	// IEEE 754 double to sortable bytes:
	// If positive (sign bit 0): flip sign bit -> all positive doubles sort correctly
	// If negative (sign bit 1): flip all bits -> negative doubles sort correctly and before positives
	uint64_t bits;
	std::memcpy(&bits, &val, 8);
	if (bits & (1ULL << 63)) {
		// Negative: flip all bits
		bits = ~bits;
	} else {
		// Positive (or zero): flip sign bit
		bits |= (1ULL << 63);
	}
	uint64_t be = KeyEncoder::host_to_be64(bits);
	dst->append(reinterpret_cast<const char*>(&be), 8);
}

double RedisZSetScoreKey::decode_double(const char* data) {
	uint64_t be;
	std::memcpy(&be, data, 8);
	uint64_t bits = KeyEncoder::be_to_host64(be);
	if (bits & (1ULL << 63)) {
		// Was positive: flip sign bit back
		bits &= ~(1ULL << 63);
	} else {
		// Was negative: flip all bits back
		bits = ~bits;
	}
	double val;
	std::memcpy(&val, &bits, 8);
	return val;
}

std::string RedisZSetScoreKey::encode(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                                      uint64_t version, double score, const std::string& member) {
	// [prefix][version:8][score:8][member]
	std::string result = RedisMetadataKey::encode(region_id, index_id, slot, user_key);

	uint64_t version_be = KeyEncoder::host_to_be64(version);
	result.append(reinterpret_cast<const char*>(&version_be), 8);

	encode_double(score, &result);

	result.append(member);

	return result;
}

std::string RedisZSetScoreKey::encode_prefix(int64_t region_id, int64_t index_id, uint16_t slot,
                                             const std::string& user_key, uint64_t version) {
	std::string result = RedisMetadataKey::encode(region_id, index_id, slot, user_key);

	uint64_t version_be = KeyEncoder::host_to_be64(version);
	result.append(reinterpret_cast<const char*>(&version_be), 8);

	return result;
}

} // namespace neokv
