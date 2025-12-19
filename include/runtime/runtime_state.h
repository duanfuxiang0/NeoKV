// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub runtime_state for neo-redis

#pragma once

#include <memory>

namespace baikaldb {

class RuntimeState {
public:
    RuntimeState() = default;
    ~RuntimeState() = default;
};

typedef std::shared_ptr<RuntimeState> SmartState;

} // namespace baikaldb
