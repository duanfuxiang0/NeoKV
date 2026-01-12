// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub transaction_pool for neo-redis

#pragma once

#include <memory>
#include <map>
#include "transaction.h"
#include "proto/store.interface.pb.h"

namespace neokv {

// Forward declaration
class Region;

// TTLInfo definition - also defined in schema_factory.h, use #ifndef guard
#ifndef NEOKV_TTLINFO_DEFINED
#define NEOKV_TTLINFO_DEFINED
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
    
    void init(int64_t /*region_id*/, bool /*use_ttl*/, int64_t /*ttl_expire*/) {
        // Stub - neo-redis doesn't use SQL transaction pool
    }
    
    void close() {
        // Stub - no cleanup needed for neo-redis
    }
    
    SmartTransaction get_transaction() {
        return std::make_shared<Transaction>();
    }
    
    SmartTransaction get_txn(uint64_t /*txn_id*/) {
        return nullptr;  // No SQL transactions in neo-redis
    }
    
    pb::TxnState get_txn_state(uint64_t /*txn_id*/) {
        return pb::TXN_ROLLBACKED;  // Default state
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
    
    int txn_commit_through_raft(uint64_t /*txn_id*/, 
                                 const pb::RegionInfo& /*region_info*/,
                                 pb::OpType /*op_type*/) {
        return 0;  // Stub - no SQL transactions
    }
    
    void remove_txn(uint64_t /*txn_id*/, bool /*mark_finished*/ = true) {
        // Stub - no SQL transactions
    }
    
    void update_primary_timestamp(uint64_t /*txn_id*/) {
        // Stub
    }
    
    int64_t get_finished_txn_affected_rows(uint64_t /*txn_id*/) {
        return 0;  // Stub
    }
    
    void rollback_mark_finished(uint64_t /*txn_id*/) {
        // Stub
    }
    
    void get_prepared_txn_info(std::unordered_map<uint64_t, pb::TransactionInfo>& /*info*/, 
                                bool /*for_num_rows*/ = false) {
        // Stub - no SQL transactions in neo-redis
    }
    
    void update_txn_num_rows_after_split(const std::vector<pb::TransactionInfo>& /*txn_infos*/) {
        // Stub
    }
    
    void clear_orphan_transactions() {
        // Stub
    }
    
    void on_leader_stop_rollback() {
        // Stub - no SQL transactions
    }
    
    void clear() {
        // Stub - clear all transactions
    }
};

} // namespace neokv
