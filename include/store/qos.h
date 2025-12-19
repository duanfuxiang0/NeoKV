// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub qos for neo-redis

#pragma once

#include <cstdint>

namespace baikaldb {

// Stub for QoS - simplified for neo-redis
class QoS {
public:
    static QoS* get_instance() {
        static QoS instance;
        return &instance;
    }
    
    void init() {}
    
    // Return true to allow all operations (no throttling for neo-redis)
    bool need_reject() { return false; }
    
    void update_statistics(int64_t /*region_id*/, int64_t /*qps*/, int64_t /*cost*/) {}
};

} // namespace baikaldb
