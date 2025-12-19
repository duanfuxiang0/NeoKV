// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub transaction_pool for neo-redis

#pragma once

#include <memory>
#include "transaction.h"

namespace baikaldb {

class TransactionPool {
public:
    static TransactionPool* get_instance() {
        static TransactionPool instance;
        return &instance;
    }
    
    SmartTransaction get_transaction() {
        return std::make_shared<Transaction>();
    }
    
    void release_transaction(SmartTransaction& txn) {
        txn.reset();
    }
};

} // namespace baikaldb
