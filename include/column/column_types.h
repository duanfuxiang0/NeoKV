// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub column types for neo-redis

#pragma once

#include <memory>
#include <vector>
#include <string>
#include <map>
#include "proto/column.pb.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {

// Stub ColumnFileInfo - use pb::ColumnFileInfo for compatibility
using ColumnFileInfo = pb::ColumnFileInfo;

// Stub ParquetFile - not used in neo-redis
class ParquetFile {
public:
    ParquetFile() = default;
    ~ParquetFile() = default;
};

// Stub ColumnFileMeta - minimal implementation for neo-redis
struct ColumnFileMeta {
    int64_t version = 0;
    std::string file_name;
};

// Stub RegionResource - contains region info for resource management
struct RegionResource {
    pb::RegionInfo region_info;
    
    RegionResource() = default;
    ~RegionResource() = default;
};

// MemTracker and SmartMemTracker are defined in memory_profile.h
// Forward declare here for compilation if memory_profile.h is not included
#ifndef BAIKALDB_MEMTRACKER_DEFINED
class MemTracker;
typedef std::shared_ptr<MemTracker> SmartMemTracker;
#endif

// Stub ExprValue - SQL expression values not used in neo-redis
// Using pb::ExprValue for compatibility
using ExprValue = pb::ExprValue;

// Stub FieldInfo - using pb::FieldInfo for compatibility
using FieldInfo = pb::FieldInfo;

// Stub ReverseIndexBase - fulltext search not used in neo-redis
class ReverseIndexBase {
public:
    ReverseIndexBase() = default;
    virtual ~ReverseIndexBase() = default;
};

// Stub ColumnFileManager - OLAP column management not used in neo-redis
class ColumnFileManager {
public:
    ColumnFileManager() = default;
    ColumnFileManager(int64_t /*region_id*/) {}
    ~ColumnFileManager() = default;
    
    static ColumnFileManager* get_instance() {
        static ColumnFileManager instance;
        return &instance;
    }
    
    void init() {}
    
    pb::ColumnStatus column_status() const { return pb::CS_NORMAL; }
    int64_t column_lines() const { return 0; }
    void manual_base_compaction() {}
    void remove_column_data(pb::ColumnStatus /*status*/ = pb::CS_NORMAL, int /*flag*/ = 0) {}
};

// Helper function for timestamp to string conversion
inline std::string timestamp_to_str(int64_t ts) {
    if (ts <= 0) {
        return "N/A";
    }
    // Simple timestamp to string conversion
    return std::to_string(ts);
}

// Helper function to check if operation type is DML
inline bool is_dml_op_type(pb::OpType type) {
    return type == pb::OP_INSERT || 
           type == pb::OP_DELETE || 
           type == pb::OP_UPDATE;
}

// Helper function to check if operation type is 2PC
inline bool is_2pc_op_type(pb::OpType type) {
    return type == pb::OP_PREPARE ||
           type == pb::OP_COMMIT ||
           type == pb::OP_ROLLBACK;
}

} // namespace baikaldb
