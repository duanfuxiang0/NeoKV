// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Split compaction filter for neo-redis
//
// After a Region split, the old Region's key range shrinks. Keys that fall
// outside the new [start_key, end_key) must be cleaned up during compaction.
// This filter checks the suffix portion of each key (after the 16-byte
// region_id+index_id prefix) against the Region's end_key to decide whether
// to drop the key.

#pragma once

#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include "rocksdb/compaction_filter.h"
#include "common.h"

namespace neokv {

namespace pb {
class RegionInfo;
}

class SplitCompactionFilter : public rocksdb::CompactionFilter {
public:
	SplitCompactionFilter() = default;

	static SplitCompactionFilter* get_instance() {
		static SplitCompactionFilter instance;
		return &instance;
	}

	const char* Name() const override {
		return "SplitCompactionFilter";
	}

	// Register a region's end_key for compaction filtering.
	// region_id: the region whose data range may have changed after split.
	// end_key: the suffix-space upper bound (exclusive). Empty means no upper bound (tail region).
	// use_ttl / ttl_expire_time: reserved for future TTL-in-compaction support.
	void set_filter_region_info(int64_t region_id, const std::string& end_key, bool /*use_ttl*/,
	                            int64_t /*ttl_expire_time*/) {
		std::lock_guard<std::mutex> lock(_mutex);
		if (end_key.empty()) {
			// Tail region — no upper bound, nothing to filter
			_region_end_keys.erase(region_id);
		} else {
			_region_end_keys[region_id] = end_key;
		}
	}

	// Overload for pb::RegionInfo (called from legacy paths; the 4-param overload is primary)
	void set_filter_region_info(const pb::RegionInfo& /*info*/) {
	}

	// Set binlog region (stub — neo-redis doesn't use binlog)
	void set_binlog_region(int64_t /*region_id*/) {
	}

	// Compaction filter: drop keys whose suffix exceeds the region's end_key.
	//
	// Key layout (data CF):
	//   [region_id:8 bytes (mem-comparable i64)][index_id:8 bytes][suffix...]
	//
	// The suffix starts at offset 16 and begins with [slot:2 bytes big-endian].
	// RegionInfo.end_key is defined in the suffix space (e.g. encode_u16be(slot_end+1)).
	// If suffix >= end_key (byte comparison), the key is outside the region's range
	// and should be dropped.
	bool Filter(int /*level*/, const rocksdb::Slice& key, const rocksdb::Slice& /*existing_value*/,
	            std::string* /*new_value*/, bool* /*value_changed*/) const override {
		// Minimum key size: 16 (prefix) + 2 (slot) = 18 bytes
		static constexpr size_t PREFIX_LEN = 16; // region_id(8) + index_id(8)
		if (key.size() < PREFIX_LEN + 2) {
			return false; // Malformed or non-Redis key, don't filter
		}

		// Extract region_id from the first 8 bytes.
		// Encoding: big-endian uint64 with sign bit flipped (XOR 1<<63).
		int64_t region_id = decode_region_id(key.data());

		std::string end_key;
		{
			std::lock_guard<std::mutex> lock(_mutex);
			auto it = _region_end_keys.find(region_id);
			if (it == _region_end_keys.end()) {
				// No end_key registered (tail region or unknown) — don't filter
				return false;
			}
			end_key = it->second;
		}

		// Compare suffix (key bytes after prefix) against end_key
		const char* suffix = key.data() + PREFIX_LEN;
		size_t suffix_len = key.size() - PREFIX_LEN;

		// If suffix >= end_key, the key is outside the region's range → drop it
		int cmp = compare_bytes(suffix, suffix_len, end_key.data(), end_key.size());
		return cmp >= 0;
	}

private:
	// Decode region_id from the first 8 bytes of a mem-comparable encoded key.
	static int64_t decode_region_id(const char* data) {
		uint64_t raw = 0;
		const auto* p = reinterpret_cast<const uint8_t*>(data);
		raw = (static_cast<uint64_t>(p[0]) << 56) | (static_cast<uint64_t>(p[1]) << 48) |
		      (static_cast<uint64_t>(p[2]) << 40) | (static_cast<uint64_t>(p[3]) << 32) |
		      (static_cast<uint64_t>(p[4]) << 24) | (static_cast<uint64_t>(p[5]) << 16) |
		      (static_cast<uint64_t>(p[6]) << 8) | static_cast<uint64_t>(p[7]);
		return static_cast<int64_t>(raw ^ (1ULL << 63));
	}

	// Byte-wise comparison (same semantics as memcmp but handles different lengths).
	static int compare_bytes(const char* a, size_t a_len, const char* b, size_t b_len) {
		size_t min_len = a_len < b_len ? a_len : b_len;
		int cmp = std::memcmp(a, b, min_len);
		if (cmp != 0) {
			return cmp;
		}
		if (a_len < b_len)
			return -1;
		if (a_len > b_len)
			return 1;
		return 0;
	}

	mutable std::mutex _mutex;
	// region_id → end_key (suffix-space exclusive upper bound)
	std::unordered_map<int64_t, std::string> _region_end_keys;
};

class SplitCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
public:
	SplitCompactionFilterFactory() = default;

	const char* Name() const override {
		return "SplitCompactionFilterFactory";
	}

	std::unique_ptr<rocksdb::CompactionFilter>
	CreateCompactionFilter(const rocksdb::CompactionFilter::Context& /*context*/) override {
		return std::unique_ptr<rocksdb::CompactionFilter>(new SplitCompactionFilter());
	}
};

} // namespace neokv
