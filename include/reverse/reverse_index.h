// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub reverse_index for neo-redis (fulltext search not supported)

#pragma once

#include <string>
#include <memory>
#include <unordered_map>

namespace baikaldb {

// Stub ReverseIndex - neo-redis doesn't support fulltext search
class ReverseIndex {
public:
    ReverseIndex() = default;
    ~ReverseIndex() = default;
};

typedef std::shared_ptr<ReverseIndex> SmartReverseIndex;

// Stub for reverse index manager
class ReverseIndexManager {
public:
    static ReverseIndexManager* get_instance() {
        static ReverseIndexManager instance;
        return &instance;
    }
};

} // namespace baikaldb
