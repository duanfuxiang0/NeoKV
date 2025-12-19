// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Simplified SstFileWriter for neo-redis

#pragma once

#include <string>
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/options.h"
#include "common.h"

namespace baikaldb {

// Wrapper around rocksdb::SstFileWriter for snapshot creation
class SstFileWriter {
public:
    SstFileWriter(const rocksdb::Options& options) 
        : _sst_writer(rocksdb::EnvOptions(), options) {}
    
    rocksdb::Status open(const std::string& file_path) {
        return _sst_writer.Open(file_path);
    }
    
    rocksdb::Status put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
        return _sst_writer.Put(key, value);
    }
    
    rocksdb::Status finish(rocksdb::ExternalSstFileInfo* file_info = nullptr) {
        return _sst_writer.Finish(file_info);
    }
    
    // Note: FileSize() is non-const in RocksDB API
    uint64_t file_size() {
        return _sst_writer.FileSize();
    }
    
private:
    rocksdb::SstFileWriter _sst_writer;
};

} // namespace baikaldb
