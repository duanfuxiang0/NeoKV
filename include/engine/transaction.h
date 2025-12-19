// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub transaction header for neo-redis (SQL transactions not supported)

#pragma once

#include <string>
#include <memory>
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

namespace baikaldb {

// Stub Transaction class - neo-redis doesn't use SQL transactions
class Transaction {
public:
    Transaction() = default;
    ~Transaction() = default;
    
    // Stub methods
    int begin() { return 0; }
    int commit() { return 0; }
    int rollback() { return 0; }
};

typedef std::shared_ptr<Transaction> SmartTransaction;

} // namespace baikaldb
