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

#include "meta_state_machine.h"

#ifdef BAIDU_INTERNAL
#include "raft/util.h"
#include "raft/storage.h"
#else
#include <braft/util.h>
#include <braft/storage.h>
#endif

#include <brpc/controller.h>

#include "cluster_manager.h"
#include "meta_server.h"
#include "meta_util.h"
#include "region_manager.h"
#include "rocks_wrapper.h"
#include "sst_file_writer.h"

namespace neokv {

DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int32(healthy_check_interval_times);

void MetaStateMachine::store_heartbeat(google::protobuf::RpcController* controller,
                                      const pb::StoreHeartBeatRequest* request,
                                      pb::StoreHeartBeatResponse* response,
                                      google::protobuf::Closure* done) {
    TimeCost time_cost;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl && cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!_is_leader.load()) {
        DB_WARNING("NOT LEADER, logid:%lu", log_id);
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(_node.leader_id().to_string());
        return;
    }
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");

    TimeCost step;
    // Generic: update instance status/capacity.
    ClusterManager::get_instance()->process_instance_heartbeat_for_store(request->instance_info());
    ClusterManager::get_instance()->process_instance_param_heartbeat_for_store(request, response);
    int64_t instance_time = step.get_time();
    step.reset();

    // Generic: peer heartbeat (load & basic bookkeeping).
    ClusterManager::get_instance()->process_peer_heartbeat_for_store(request, response);
    int64_t peer_balance_time = step.get_time();
    step.reset();

    // Generic: region heartbeat (new regions, version/peer changes).
    RegionManager::get_instance()->check_whether_illegal_peer(request, response);
    int64_t peer_time = step.get_time();
    step.reset();

    RegionManager::get_instance()->leader_heartbeat_for_region(request, response);
    int64_t leader_region_time = step.get_time();
    step.reset();

    int64_t ts = butil::gettimeofday_us();
    RegionManager::get_instance()->update_leader_status(request, ts);
    int64_t leader_status_time = step.get_time();

    _store_heart_beat << time_cost.get_time();
    DB_NOTICE("store:%s heart beat, time_cost:%ld, instance_time:%ld, peer_balance_time:%ld, peer_time:%ld, "
              "leader_region_time:%ld, leader_status_time:%ld, log_id:%lu",
              request->instance_info().address().c_str(),
              time_cost.get_time(),
              instance_time, peer_balance_time, peer_time,
              leader_region_time, leader_status_time, log_id);
}

void MetaStateMachine::neo_heartbeat(google::protobuf::RpcController* controller,
                                       const pb::BaikalHeartBeatRequest* request,
                                       pb::BaikalHeartBeatResponse* response,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl && cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!_is_leader.load()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(_node.leader_id().to_string());
        DB_WARNING("NOT LEADER neo_heartbeat, logid:%lu", log_id);
        return;
    }
    // Neo-redis: neo_heartbeat is unused; keep success stub.
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    (void)request;
}

void MetaStateMachine::neo_other_heartbeat(google::protobuf::RpcController* controller,
                                             const pb::BaikalOtherHeartBeatRequest* request,
                                             pb::BaikalOtherHeartBeatResponse* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl && cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!_is_leader.load()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(_node.leader_id().to_string());
        DB_WARNING("NOT LEADER neo_other_heartbeat, logid:%lu", log_id);
        return;
    }
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    (void)request;
}

void MetaStateMachine::console_heartbeat(google::protobuf::RpcController* controller,
                                        const pb::ConsoleHeartBeatRequest* request,
                                        pb::ConsoleHeartBeatResponse* response,
                                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl && cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!_is_leader.load()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(_node.leader_id().to_string());
        DB_WARNING("NOT LEADER console_heartbeat, logid:%lu", log_id);
        return;
    }
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    (void)request;
}

void MetaStateMachine::healthy_check_function() {
    // Neo-redis: SQL-related health checks removed. Keep minimal stub.
}

bool MetaStateMachine::whether_can_decide() {
    // After a few heartbeat intervals as leader, allow decision-making.
    return butil::gettimeofday_us() - _leader_start_timestmap >
           (int64_t)3 * FLAGS_store_heart_beat_interval_us;
}

void MetaStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::Closure* done = iter.done();
        brpc::ClosureGuard done_guard(done);
        if (done) {
            ((MetaServerClosure*)done)->raft_time_cost = ((MetaServerClosure*)done)->time_cost.get_time();
        }
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        pb::MetaManagerRequest request;
        if (!request.ParseFromZeroCopyStream(&wrapper)) {
            DB_FATAL("parse from protobuf fail when on_apply");
            IF_DONE_SET_RESPONSE(done, pb::PARSE_FROM_PB_FAIL, "parse from protobuf fail");
            if (done) {
                braft::run_closure_in_bthread(done_guard.release());
            }
            continue;
        }
        if (done && ((MetaServerClosure*)done)->response) {
            ((MetaServerClosure*)done)->response->set_op_type(request.op_type());
        }
        DB_NOTICE("on apply, term:%ld, index:%ld, request op_type:%s",
                  iter.term(), iter.index(), pb::OpType_Name(request.op_type()).c_str());

        switch (request.op_type()) {
        // Cluster ops (generic)
        case pb::OP_ADD_LOGICAL:
            ClusterManager::get_instance()->add_logical(request, done);
            break;
        case pb::OP_ADD_PHYSICAL:
            ClusterManager::get_instance()->add_physical(request, done);
            break;
        case pb::OP_ADD_INSTANCE:
            ClusterManager::get_instance()->add_instance(request, done);
            break;
        case pb::OP_DROP_PHYSICAL:
            ClusterManager::get_instance()->drop_physical(request, done);
            break;
        case pb::OP_DROP_LOGICAL:
            ClusterManager::get_instance()->drop_logical(request, done);
            break;
        case pb::OP_DROP_INSTANCE:
            ClusterManager::get_instance()->drop_instance(request, done);
            break;
        case pb::OP_UPDATE_INSTANCE:
            ClusterManager::get_instance()->update_instance(request, done);
            break;
        case pb::OP_UPDATE_INSTANCE_PARAM:
            ClusterManager::get_instance()->update_instance_param(request, done);
            break;
        case pb::OP_MOVE_PHYSICAL:
            ClusterManager::get_instance()->move_physical(request, done);
            break;

        // Region ops (generic)
        case pb::OP_DROP_REGION:
            RegionManager::get_instance()->drop_region(request, iter.index(), done);
            break;
        case pb::OP_UPDATE_REGION:
            RegionManager::get_instance()->update_region(request, iter.index(), done);
            break;
        case pb::OP_SPLIT_REGION:
            // Allocates new region_id(s) by advancing max_region_id in raft.
            RegionManager::get_instance()->split_region(request, done);
            break;

        default:
            DB_FATAL("unsupport request type, type:%d", request.op_type());
            IF_DONE_SET_RESPONSE(done, pb::UNSUPPORT_REQ_TYPE, "unsupport request type");
            break;
        }

        _applied_index = iter.index();
        if (done) {
            braft::run_closure_in_bthread(done_guard.release());
        }
    }
}

void MetaStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    DB_WARNING("start on snapshot save");
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = false;
    read_options.total_order_seek = true;
    auto iter = RocksWrapper::get_instance()->new_iterator(read_options,
                                                           RocksWrapper::get_instance()->get_meta_info_handle());
    iter->SeekToFirst();
    Bthread bth(&BTHREAD_ATTR_SMALL);
    bth.run([this, done, iter, writer]() { save_snapshot(done, iter, writer); });
}

void MetaStateMachine::save_snapshot(braft::Closure* done,
                                    rocksdb::Iterator* iter,
                                    braft::SnapshotWriter* writer) {
    brpc::ClosureGuard done_guard(done);
    std::unique_ptr<rocksdb::Iterator> iter_lock(iter);

    std::string snapshot_path = writer->get_path();
    std::string sst_file_path = snapshot_path + "/meta_info.sst";
    rocksdb::Options option = RocksWrapper::get_instance()->get_options(
        RocksWrapper::get_instance()->get_meta_info_handle());
    SstFileWriter sst_writer(option);

    auto s = sst_writer.open(sst_file_path);
    if (!s.ok()) {
        DB_WARNING("open SstFileWriter failed: %s", s.ToString().c_str());
        done->status().set_error(EINVAL, "Fail to open SstFileWriter");
        return;
    }
    for (; iter->Valid(); iter->Next()) {
        auto res = sst_writer.put(iter->key(), iter->value());
        if (!res.ok()) {
            DB_WARNING("write SstFileWriter failed: %s", res.ToString().c_str());
            done->status().set_error(EINVAL, "Fail to write SstFileWriter");
            return;
        }
    }
    s = sst_writer.finish();
    if (!s.ok()) {
        DB_WARNING("finish SstFileWriter failed: %s", s.ToString().c_str());
        done->status().set_error(EINVAL, "Fail to finish SstFileWriter");
        return;
    }
    if (writer->add_file("/meta_info.sst") != 0) {
        done->status().set_error(EINVAL, "Fail to add file");
        DB_WARNING("add snapshot file failed");
        return;
    }
}

int MetaStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    DB_WARNING("start on snapshot load");

    // Clear existing meta info in rocksdb.
    std::string remove_start_key(MetaServer::CLUSTER_IDENTIFY);
    rocksdb::WriteOptions options;
    auto status = RocksWrapper::get_instance()->remove_range(options,
                                                             RocksWrapper::get_instance()->get_meta_info_handle(),
                                                             remove_start_key,
                                                             MetaServer::MAX_IDENTIFY,
                                                             false);
    if (!status.ok()) {
        DB_FATAL("remove_range error when on snapshot load: %s", status.ToString().c_str());
        return -1;
    }

    std::vector<std::string> files;
    reader->list_files(&files);
    for (auto& file : files) {
        if (file != "/meta_info.sst") {
            continue;
        }
        std::string snapshot_path = reader->get_path();
        _applied_index = parse_snapshot_index_from_path(snapshot_path, false);
        snapshot_path.append("/meta_info.sst");

        rocksdb::IngestExternalFileOptions ifo;
        auto res = RocksWrapper::get_instance()->ingest_external_file(
            RocksWrapper::get_instance()->get_meta_info_handle(), {snapshot_path}, ifo);
        if (!res.ok()) {
            DB_WARNING("ingest snapshot sst failed: %s", res.ToString().c_str());
            return -1;
        }

        // Restore in-memory state.
        int ret = ClusterManager::get_instance()->load_snapshot();
        if (ret != 0) {
            DB_FATAL("ClusterManager load_snapshot fail");
            return -1;
        }

        RegionManager::get_instance()->clear();
        // Load max_region_id.
        {
            std::string key = MetaServer::SCHEMA_IDENTIFY + MetaServer::MAX_ID_SCHEMA_IDENTIFY + "max_region_id";
            std::string value;
            auto s = RocksWrapper::get_instance()->get(rocksdb::ReadOptions(),
                                                       RocksWrapper::get_instance()->get_meta_info_handle(),
                                                       key, &value);
            if (s.ok() && value.size() == sizeof(int64_t)) {
                int64_t max_region_id = *(reinterpret_cast<const int64_t*>(value.data()));
                RegionManager::get_instance()->set_max_region_id(max_region_id);
            }
        }
        // Load all regions.
        {
            rocksdb::ReadOptions ro;
            ro.total_order_seek = true;
            std::unique_ptr<rocksdb::Iterator> it(RocksWrapper::get_instance()->new_iterator(
                ro, RocksWrapper::get_instance()->get_meta_info_handle()));
            std::string prefix = MetaServer::SCHEMA_IDENTIFY + MetaServer::REGION_SCHEMA_IDENTIFY;
            for (it->Seek(prefix); it->Valid(); it->Next()) {
                auto k = it->key();
                if (!k.starts_with(prefix)) {
                    break;
                }
                RegionManager::get_instance()->load_region_snapshot(k.ToString(), it->value().ToString());
            }
        }
        break;
    }

    set_have_data(true);
    // Placeholder incremental region info to avoid full-sync paths (best-effort).
    int64_t incr_index = _applied_index + 1;
    std::vector<pb::RegionInfo> region_infos;
    RegionManager::get_instance()->put_incremental_regioninfo(incr_index, region_infos);
    return 0;
}

void MetaStateMachine::on_leader_start() {
    DB_WARNING("leader start at new term");
    ClusterManager::get_instance()->reset_instance_status();
    RegionManager::get_instance()->reset_region_status();
    _leader_start_timestmap = butil::gettimeofday_us();
    CommonStateMachine::on_leader_start();
    _is_leader.store(true);
}

void MetaStateMachine::on_leader_stop() {
    _is_leader.store(false);
    set_global_load_balance(true);
    set_global_migrate(true);
    _unsafe_decision = false;
    DB_WARNING("leader stop");
    CommonStateMachine::on_leader_stop();
}

} // namespace neokv


