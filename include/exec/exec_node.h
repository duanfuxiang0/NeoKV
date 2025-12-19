// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub exec_node for neo-redis

#pragma once

#include <memory>

namespace baikaldb {

class ExecNode {
public:
    ExecNode() = default;
    virtual ~ExecNode() = default;
};

typedef std::shared_ptr<ExecNode> SmartExecNode;

} // namespace baikaldb
