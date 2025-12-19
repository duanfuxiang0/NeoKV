// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub runtime_state_pool for neo-redis

#pragma once

#include "runtime_state.h"

namespace baikaldb {

class RuntimeStatePool {
public:
    static RuntimeStatePool* get_instance() {
        static RuntimeStatePool instance;
        return &instance;
    }
};

} // namespace baikaldb
