// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub transaction header for neo-redis (SQL transactions not supported)

#pragma once

#include <string>
#include <memory>
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

// Forward declarations
namespace neokv {
struct IndexInfo;
}

namespace neokv {

// Note: GetMode enum is defined in rocks_wrapper.h

// Stub Transaction class - neo-redis doesn't use SQL transactions
class Transaction {
public:
    Transaction() = default;
    ~Transaction() = default;
    
    // Transaction ID for tracking (methods for compatibility with closure.cpp)
    uint64_t txn_id() const { return _txn_id; }
    int64_t seq_id() const { return _seq_id; }
    void set_txn_id(uint64_t id) { _txn_id = id; }
    void set_seq_id(int64_t id) { _seq_id = id; }
    
    // Stub methods
    int begin() { return 0; }
    int begin(const rocksdb::TransactionOptions& /*opt*/) { return 0; }
    rocksdb::Status commit() { return rocksdb::Status::OK(); }
    rocksdb::Status rollback() { return rocksdb::Status::OK(); }
    
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
    
    void set_in_process(bool in_process) {
        _in_process = in_process;
    }
    
    bool in_process() const {
        return _in_process;
    }
    
    void clear_raftreq() {
        // Stub
    }
    
    void reset_active_time() {
        // Stub
    }
    
    bool load_last_response() {
        return false;  // No cached response
    }
    
    int64_t dml_num_affected_rows() const {
        return 0;
    }
    
    int err_code() const {
        return 0;
    }
    
    bool txn_set_process_cas(bool expected, bool desired) {
        if (_in_process == expected) {
            _in_process = desired;
            return true;
        }
        return false;
    }
    
    bool is_finished() const {
        return _is_finished;
    }
    
    void set_finished(bool finished) {
        _is_finished = finished;
    }
    
    bool has_dml_executed() const {
        return false;  // No SQL DML in neo-redis
    }
    
    rocksdb::Transaction* get_txn() {
        return nullptr;  // Stub - no underlying RocksDB transaction
    }
    
    void set_resource(void* /*resource*/) {
        // Stub
    }
    
    void remove_columns(const rocksdb::Slice& /*key*/) {
        // Stub - no column store in neo-redis
    }
    
    // Check if key fits region range - stub for neo-redis
    static bool fits_region_range(const rocksdb::Slice& /*key*/, 
                                   const std::string& /*start_key*/,
                                   const std::string& /*end_key*/) {
        return true;  // Always fits in neo-redis
    }
    
    // Extended version with index info
    static bool fits_region_range(const rocksdb::Slice& /*key*/, 
                                   const std::string& /*value*/,
                                   const std::string* /*start_key*/,
                                   const std::string* /*end_key*/,
                                   const IndexInfo& /*pk_info*/,
                                   const IndexInfo& /*index_info*/) {
        return true;  // Always fits in neo-redis
    }

private:
    uint64_t _txn_id = 0;
    int64_t _seq_id = 0;
    bool _in_process = false;
    bool _is_finished = false;
};

typedef std::shared_ptr<Transaction> SmartTransaction;

} // namespace neokv
