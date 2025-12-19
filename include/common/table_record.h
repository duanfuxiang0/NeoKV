// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub table_record for neo-redis

#pragma once

#include <memory>
#include <string>

namespace baikaldb {

class TableRecord {
public:
    TableRecord() = default;
    ~TableRecord() = default;
};

typedef std::shared_ptr<TableRecord> SmartRecord;

} // namespace baikaldb
