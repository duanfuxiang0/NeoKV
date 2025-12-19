// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub runtime_state_pool for neo-redis

#pragma once

#include <map>
#include <unordered_map>
#include <mutex>
#include "runtime_state.h"

namespace baikaldb {

class RuntimeStatePool {
public:
    RuntimeStatePool() = default;
    ~RuntimeStatePool() = default;
    
    static RuntimeStatePool* get_instance() {
        static RuntimeStatePool instance;
        return &instance;
    }
    
    // Get state map (stub - returns empty map for neo-redis)
    std::unordered_map<uint64_t, SmartState> get_state_map() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _state_map;
    }
    
private:
    std::mutex _mutex;
    std::unordered_map<uint64_t, SmartState> _state_map;
};

} // namespace baikaldb
