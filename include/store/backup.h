// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub backup for neo-redis

#pragma once

#include <string>
#include <memory>
#include <vector>
#include <brpc/controller.h>

namespace neokv {

class Region;

// SstBackupType is defined in common.h

class Backup {
public:
    Backup() = default;
    ~Backup() = default;
    
    void set_info(std::shared_ptr<Region> /*region*/, int64_t /*region_id*/) {
        // Stub - neo-redis doesn't use SQL backup
    }
    
    void process_download_sst(brpc::Controller* /*cntl*/, 
                              std::vector<std::string>& /*request_vec*/,
                              SstBackupType /*backup_type*/) {
        // Stub - neo-redis doesn't use SQL backup
    }
    
    int process_upload_sst(brpc::Controller* /*cntl*/, bool /*ingest_store_latest_sst*/) {
        return -1;  // Not supported in neo-redis
    }
};

} // namespace neokv
