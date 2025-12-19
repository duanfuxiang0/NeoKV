// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub transaction_pool for neo-redis

#pragma once

#include <memory>
#include <map>
#include "transaction.h"

namespace baikaldb {

// Forward declaration
class Region;

// TTLInfo definition - also defined in schema_factory.h, use #ifndef guard
#ifndef BAIKALDB_TTLINFO_DEFINED
#define BAIKALDB_TTLINFO_DEFINED
struct TTLInfo {
    int64_t ttl_duration_s = 0;
    int64_t online_ttl_expire_time_us = 0;
};
#endif

class TransactionPool {
public:
    static TransactionPool* get_instance() {
        static TransactionPool instance;
        return &instance;
    }
    
    TransactionPool() = default;
    ~TransactionPool() = default;
    
    void close() {
        // Stub - no cleanup needed for neo-redis
    }
    
    SmartTransaction get_transaction() {
        return std::make_shared<Transaction>();
    }
    
    void release_transaction(SmartTransaction& txn) {
        txn.reset();
    }
    
    int64_t num_prepared() const {
        return 0;  // No SQL transactions
    }
    
    int64_t num_began() const {
        return 0;  // No SQL transactions
    }
    
    void clear_transactions(Region* /*region*/ = nullptr) {
        // Stub - no transactions to clear in neo-redis
    }
    
    void update_ttl_info(const TTLInfo& /*info*/) {
        // Stub - TTL handled by RedisTTLCleaner
    }
    
    void update_ttl_info(bool /*online*/, int64_t /*ttl_duration*/) {
        // Stub - TTL handled by RedisTTLCleaner
    }
    
    void rollback_txn_before(int64_t /*timestamp*/) {
        // Stub - no SQL transactions
    }
};

} // namespace baikaldb
