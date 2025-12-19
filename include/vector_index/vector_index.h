// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub vector_index for neo-redis

#pragma once

#include <memory>

namespace baikaldb {

class VectorIndex {
public:
    VectorIndex() = default;
    ~VectorIndex() = default;
};

typedef std::shared_ptr<VectorIndex> SmartVectorIndex;

} // namespace baikaldb
