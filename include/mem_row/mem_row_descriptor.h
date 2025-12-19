// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub mem_row_descriptor for neo-redis

#pragma once

#include <memory>

namespace baikaldb {

class MemRowDescriptor {
public:
    MemRowDescriptor() = default;
    ~MemRowDescriptor() = default;
};

typedef std::shared_ptr<MemRowDescriptor> SmartDescriptor;

} // namespace baikaldb
