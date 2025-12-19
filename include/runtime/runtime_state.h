// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub runtime_state for neo-redis

#pragma once

#include <memory>
#include <cstdint>

namespace baikaldb {

class RuntimeState {
public:
    RuntimeState() = default;
    ~RuntimeState() = default;
    
    // Sign for query identification (public member for compatibility)
    uint64_t sign = 0;
    
    // Cancel query (stub - no-op for neo-redis)
    void cancel() {}
};

typedef std::shared_ptr<RuntimeState> SmartState;

} // namespace baikaldb
