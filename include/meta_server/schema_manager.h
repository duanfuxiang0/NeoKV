// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include "proto/meta.interface.pb.h"
#include "meta_state_machine.h"

namespace neokv {
// Neo-redis: SchemaManager is trimmed to only keep region split/merge and region meta updates.
typedef std::shared_ptr<pb::RegionInfo> SmartRegionInfo;
class SchemaManager {
public:
    static const std::string MAX_NAMESPACE_ID_KEY;
    static const std::string MAX_DATABASE_ID_KEY;
    static const std::string MAX_TABLE_ID_KEY;
    static const std::string MAX_REGION_ID_KEY;
    static SchemaManager* get_instance() {
        static SchemaManager instance;
        return &instance;
    }
    ~SchemaManager() {}
    void process_schema_info(google::protobuf::RpcController* controller,
                  const pb::MetaManagerRequest* request, 
                  pb::MetaManagerResponse* response,
                  google::protobuf::Closure* done); 
    void set_meta_state_machine(MetaStateMachine* meta_state_machine) {
        _meta_state_machine = meta_state_machine;
    }
    bool get_unsafe_decision() {
        return _meta_state_machine->get_unsafe_decision();
    }
private:
    SchemaManager() {}
    int pre_process_for_merge_region(const pb::MetaManagerRequest* request,
                                    pb::MetaManagerResponse* response,
                                    uint64_t log_id);
    int pre_process_for_split_region(const pb::MetaManagerRequest* request, 
                                    pb::MetaManagerResponse* response,
                                    uint64_t log_id);

    MetaStateMachine* _meta_state_machine;
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
