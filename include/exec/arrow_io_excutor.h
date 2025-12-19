// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub arrow_io_excutor for neo-redis

#pragma once

namespace baikaldb {

// Stub for Arrow executor - not used in neo-redis
class GlobalArrowExecutor {
public:
    static GlobalArrowExecutor* get_instance() {
        static GlobalArrowExecutor instance;
        return &instance;
    }
    
    int init() { return 0; }
};

} // namespace baikaldb
