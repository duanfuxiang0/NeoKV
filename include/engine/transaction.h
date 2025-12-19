// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub transaction header for neo-redis (SQL transactions not supported)

#pragma once

#include <string>
#include <memory>
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

namespace baikaldb {

// Note: GetMode enum is defined in rocks_wrapper.h

// Stub Transaction class - neo-redis doesn't use SQL transactions
class Transaction {
public:
    Transaction() = default;
    ~Transaction() = default;
    
    // Transaction ID for tracking (methods for compatibility with closure.cpp)
    uint64_t txn_id() const { return _txn_id; }
    int64_t seq_id() const { return _seq_id; }
    
    // Stub methods
    int begin() { return 0; }
    int commit() { return 0; }
    int rollback() { return 0; }
    
    // Put methods used by meta_writer
    int put_meta_info(const std::string& key, const std::string& value) {
        // Stub: in real neo-redis, this would be handled differently
        return 0;
    }
    
    int get_meta_info(const std::string& key, std::string* value) {
        // Stub
        return -1;  // Not found
    }
    
    // Get for update - stub for SQL transaction locking
    int get_for_update(const std::string& /*key*/, std::string* /*value*/, 
                       int /*mode*/ = 2) {
        return 0;  // Stub - no locking in neo-redis
    }
    
    // Additional methods for closure.cpp compatibility
    void rollback_current_request() {
        // Stub - no rollback in neo-redis
    }
    
    void clear_current_req_point_seq() {
        // Stub
    }
    
    void set_in_process(bool /*in_process*/) {
        // Stub
    }
    
    void clear_raftreq() {
        // Stub
    }

private:
    uint64_t _txn_id = 0;
    int64_t _seq_id = 0;
};

typedef std::shared_ptr<Transaction> SmartTransaction;

} // namespace baikaldb
