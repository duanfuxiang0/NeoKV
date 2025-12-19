// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub transaction_db_bthread_mutex for neo-redis

#pragma once

#include "rocksdb/utilities/transaction_db.h"

namespace baikaldb {

// Neo-redis: No custom mutex factory needed
// TransactionDB will use its default mutex implementation

// Stub class - not actually used but keeps compilation compatible
class TransactionDBBthreadFactory {
public:
    // This stub is here only for compilation compatibility
    // The actual mutex factory is set to nullptr in rocks_wrapper.cpp
};

} // namespace baikaldb
