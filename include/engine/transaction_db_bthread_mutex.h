// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub transaction_db_bthread_mutex for neo-redis

#pragma once

#include "rocksdb/utilities/transaction_db.h"

namespace baikaldb {

// Neo-redis: No special mutex needed, use standard TransactionDB
inline rocksdb::TransactionDB* create_transaction_db(
    const rocksdb::Options& options,
    const rocksdb::TransactionDBOptions& txn_db_options,
    const std::string& path,
    rocksdb::TransactionDB** dbptr) {
    rocksdb::Status s = rocksdb::TransactionDB::Open(options, txn_db_options, path, dbptr);
    if (!s.ok()) {
        return nullptr;
    }
    return *dbptr;
}

} // namespace baikaldb
