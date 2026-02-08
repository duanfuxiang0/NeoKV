// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub table_record for neo-redis

#pragma once

#include <memory>
#include <string>
#include <cstdint>

namespace neokv {

// Stub TableRecord - SQL record structure not used in neo-redis
class TableRecord {
public:
	TableRecord() = default;
	~TableRecord() = default;
};

typedef std::shared_ptr<TableRecord> SmartRecord;

inline SmartRecord create_record() {
	return std::make_shared<TableRecord>();
}

} // namespace neokv
