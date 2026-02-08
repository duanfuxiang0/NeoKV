// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Simplified split compaction filter for neo-redis

#pragma once

#include "rocksdb/compaction_filter.h"
#include "common.h"

namespace neokv {

// Simplified compaction filter - just passes through all keys for neo-redis
// Forward declaration
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

	// Set filter region info (stub - no filtering in neo-redis)
	// Overload with 4 parameters: region_id, end_key, use_ttl, ttl_expire_time
	void set_filter_region_info(int64_t /*region_id*/, const std::string& /*end_key*/, bool /*use_ttl*/,
	                            int64_t /*ttl_expire_time*/) {
		// Stub - no split compaction filtering in neo-redis
	}

	// Overload for pb::RegionInfo
	void set_filter_region_info(const pb::RegionInfo& /*info*/) {
		// Stub - no split compaction filtering in neo-redis
	}

	// Set binlog region (stub - not used in neo-redis)
	void set_binlog_region(int64_t /*region_id*/) {
		// Stub - neo-redis doesn't use binlog
	}

	// For neo-redis, we don't filter any keys during compaction
	// All key filtering is handled by TTL cleanup
	bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& existing_value, std::string* new_value,
	            bool* value_changed) const override {
		return false; // Don't filter any keys
	}
};

class SplitCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
public:
	SplitCompactionFilterFactory() = default;

	const char* Name() const override {
		return "SplitCompactionFilterFactory";
	}

	std::unique_ptr<rocksdb::CompactionFilter>
	CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context) override {
		return std::unique_ptr<rocksdb::CompactionFilter>(new SplitCompactionFilter());
	}
};

} // namespace neokv
