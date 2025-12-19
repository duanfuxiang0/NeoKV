// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Simplified split compaction filter for neo-redis

#pragma once

#include "rocksdb/compaction_filter.h"
#include "common.h"

namespace baikaldb {

// Simplified compaction filter - just passes through all keys for neo-redis
class SplitCompactionFilter : public rocksdb::CompactionFilter {
public:
    SplitCompactionFilter() = default;
    
    const char* Name() const override {
        return "SplitCompactionFilter";
    }
    
    // For neo-redis, we don't filter any keys during compaction
    // All key filtering is handled by TTL cleanup
    bool Filter(int level,
                const rocksdb::Slice& key,
                const rocksdb::Slice& existing_value,
                std::string* new_value,
                bool* value_changed) const override {
        return false;  // Don't filter any keys
    }
};

class SplitCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
public:
    SplitCompactionFilterFactory() = default;
    
    const char* Name() const override {
        return "SplitCompactionFilterFactory";
    }
    
    std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& context) override {
        return std::unique_ptr<rocksdb::CompactionFilter>(new SplitCompactionFilter());
    }
};

} // namespace baikaldb
