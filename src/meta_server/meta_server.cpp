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

#include "meta_server.h"

#include <brpc/controller.h>

#include "cluster_manager.h"
#include "meta_rocksdb.h"
#include "meta_state_machine.h"
#include "meta_util.h"
#include "rocks_wrapper.h"
#include "schema_manager.h"
#include "tso_state_machine.h"

namespace neokv {

DEFINE_int32(meta_port, 8010, "Meta port");
DEFINE_int32(meta_replica_number, 3, "Meta replica num");
DEFINE_int32(concurrency_num, 40, "concurrency num, default: 40");
DECLARE_int64(flush_memtable_interval_us);

const std::string MetaServer::CLUSTER_IDENTIFY(1, 0x01);
const std::string MetaServer::LOGICAL_CLUSTER_IDENTIFY(1, 0x01);
const std::string MetaServer::LOGICAL_KEY = "logical_room";
const std::string MetaServer::PHYSICAL_CLUSTER_IDENTIFY(1, 0x02);
const std::string MetaServer::INSTANCE_CLUSTER_IDENTIFY(1, 0x03);
const std::string MetaServer::INSTANCE_PARAM_CLUSTER_IDENTIFY(1, 0x04);

const std::string MetaServer::PRIVILEGE_IDENTIFY(1, 0x03);

const std::string MetaServer::SCHEMA_IDENTIFY(1, 0x02);
const std::string MetaServer::MAX_ID_SCHEMA_IDENTIFY(1, 0x01);
const std::string MetaServer::NAMESPACE_SCHEMA_IDENTIFY(1, 0x02);
const std::string MetaServer::DATABASE_SCHEMA_IDENTIFY(1, 0x03);
const std::string MetaServer::TABLE_SCHEMA_IDENTIFY(1, 0x04);
const std::string MetaServer::REGION_SCHEMA_IDENTIFY(1, 0x05);

const std::string MetaServer::DDLWORK_IDENTIFY(1, 0x06);
const std::string MetaServer::STATISTICS_IDENTIFY(1, 0x07);
const std::string MetaServer::INDEX_DDLWORK_REGION_IDENTIFY(1, 0x08);
const std::string MetaServer::MAX_IDENTIFY(1, 0xFF);

MetaServer::~MetaServer() {
}

int MetaServer::init(const std::vector<braft::PeerId>& peers) {
	int ret = MetaRocksdb::get_instance()->init();
	if (ret < 0) {
		DB_FATAL("rocksdb init fail");
		return -1;
	}

	butil::EndPoint addr;
	addr.ip = butil::my_ip();
	addr.port = FLAGS_meta_port;
	braft::PeerId peer_id(addr, 0);

	_meta_state_machine = new (std::nothrow) MetaStateMachine(peer_id);
	if (_meta_state_machine == nullptr) {
		DB_FATAL("new meta_state_machine fail");
		return -1;
	}
	// State machine initialization hooks.
	ClusterManager::get_instance()->set_meta_state_machine(_meta_state_machine);
	SchemaManager::get_instance()->set_meta_state_machine(_meta_state_machine);

	ret = _meta_state_machine->init(peers);
	if (ret != 0) {
		DB_FATAL("meta state machine init fail");
		return -1;
	}
	DB_WARNING("meta state machine init success");

	_tso_state_machine = new (std::nothrow) TSOStateMachine(peer_id);
	if (_tso_state_machine == nullptr) {
		DB_FATAL("new tso_state_machine fail");
		return -1;
	}
	ret = _tso_state_machine->init(peers);
	if (ret != 0) {
		DB_FATAL("tso_state_machine init fail");
		return -1;
	}
	DB_WARNING("tso_state_machine init success");

	_flush_bth.run([this]() { flush_memtable_thread(); });
	_init_success = true;
	return 0;
}

void MetaServer::flush_memtable_thread() {
	while (!_shutdown) {
		bthread_usleep_fast_shutdown(FLAGS_flush_memtable_interval_us, _shutdown);
		if (_shutdown) {
			return;
		}
		auto rocksdb = RocksWrapper::get_instance();
		rocksdb::FlushOptions flush_options;
		auto status = rocksdb->flush(flush_options, rocksdb->get_meta_info_handle());
		if (!status.ok()) {
			DB_WARNING("flush meta info to rocksdb fail: %s", status.ToString().c_str());
		}
		status = rocksdb->flush(flush_options, rocksdb->get_raft_log_handle());
		if (!status.ok()) {
			DB_WARNING("flush log_cf to rocksdb fail: %s", status.ToString().c_str());
		}
	}
}

void MetaServer::meta_manager(google::protobuf::RpcController* controller, const pb::MetaManagerRequest* request,
                              pb::MetaManagerResponse* response, google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	RETURN_IF_NOT_INIT(_init_success, response, log_id);
	if (request == nullptr) {
		ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "null request", pb::OP_NONE, log_id);
		return;
	}

	// Cluster ops (generic).
	if (request->op_type() == pb::OP_ADD_PHYSICAL || request->op_type() == pb::OP_ADD_LOGICAL ||
	    request->op_type() == pb::OP_ADD_INSTANCE || request->op_type() == pb::OP_DROP_PHYSICAL ||
	    request->op_type() == pb::OP_DROP_LOGICAL || request->op_type() == pb::OP_DROP_INSTANCE ||
	    request->op_type() == pb::OP_UPDATE_INSTANCE || request->op_type() == pb::OP_UPDATE_INSTANCE_PARAM ||
	    request->op_type() == pb::OP_MOVE_PHYSICAL) {
		ClusterManager::get_instance()->process_cluster_info(controller, request, response, done_guard.release());
		return;
	}

	// Region ops (neo-redis core).
	if (request->op_type() == pb::OP_UPDATE_REGION || request->op_type() == pb::OP_DROP_REGION ||
	    request->op_type() == pb::OP_SPLIT_REGION || request->op_type() == pb::OP_MERGE_REGION) {
		SchemaManager::get_instance()->process_schema_info(controller, request, response, done_guard.release());
		return;
	}

	// Runtime toggles (generic).
	if (request->op_type() == pb::OP_OPEN_LOAD_BALANCE || request->op_type() == pb::OP_CLOSE_LOAD_BALANCE) {
		response->set_errcode(pb::SUCCESS);
		response->set_errmsg("success");
		response->set_op_type(request->op_type());
		const bool open = request->op_type() == pb::OP_OPEN_LOAD_BALANCE;
		if (request->resource_tags_size() == 0) {
			_meta_state_machine->set_global_load_balance(open);
			DB_WARNING("%s global load balance", open ? "open" : "close");
			return;
		}
		for (auto& resource_tag : request->resource_tags()) {
			_meta_state_machine->set_load_balance(resource_tag, open);
			DB_WARNING("%s load balance for resource_tag: %s", open ? "open" : "close", resource_tag.c_str());
		}
		return;
	}
	if (request->op_type() == pb::OP_OPEN_MIGRATE || request->op_type() == pb::OP_CLOSE_MIGRATE) {
		response->set_errcode(pb::SUCCESS);
		response->set_errmsg("success");
		response->set_op_type(request->op_type());
		const bool open = request->op_type() == pb::OP_OPEN_MIGRATE;
		if (request->resource_tags_size() == 0) {
			_meta_state_machine->set_global_migrate(open);
			DB_WARNING("%s global migrate", open ? "open" : "close");
			return;
		}
		for (auto& resource_tag : request->resource_tags()) {
			_meta_state_machine->set_migrate(resource_tag, open);
			DB_WARNING("%s migrate for resource_tag: %s", open ? "open" : "close", resource_tag.c_str());
		}
		return;
	}
	if (request->op_type() == pb::OP_OPEN_UNSAFE_DECISION) {
		_meta_state_machine->set_unsafe_decision(true);
		response->set_errcode(pb::SUCCESS);
		response->set_op_type(request->op_type());
		DB_WARNING("open unsafe decision");
		return;
	}
	if (request->op_type() == pb::OP_CLOSE_UNSAFE_DECISION) {
		_meta_state_machine->set_unsafe_decision(false);
		response->set_errcode(pb::SUCCESS);
		response->set_op_type(request->op_type());
		DB_WARNING("close unsafe decision");
		return;
	}

	ERROR_SET_RESPONSE(response, pb::UNSUPPORT_REQ_TYPE, "unsupported op_type in neo-redis meta", request->op_type(),
	                   log_id);
}

void MetaServer::query(google::protobuf::RpcController* controller, const pb::QueryRequest* request,
                       pb::QueryResponse* response, google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	RETURN_IF_NOT_INIT(_init_success, response, log_id);
	response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
	response->set_errmsg("neo-redis meta query not supported");
	(void)request;
}

void MetaServer::raft_control(google::protobuf::RpcController* controller, const pb::RaftControlRequest* request,
                              pb::RaftControlResponse* response, google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	RETURN_IF_NOT_INIT(_init_success, response, log_id);
	// region_id: 0 -> meta; 2 -> tso.
	if (request->region_id() == 0) {
		_meta_state_machine->raft_control(controller, request, response, done_guard.release());
		return;
	}
	if (request->region_id() == 2) {
		_tso_state_machine->raft_control(controller, request, response, done_guard.release());
		return;
	}
	response->set_region_id(request->region_id());
	response->set_errcode(pb::INPUT_PARAM_ERROR);
	response->set_errmsg("unmatch region id");
	DB_FATAL("unmatch region_id in meta server, request: %s", request->ShortDebugString().c_str());
}

void MetaServer::store_heartbeat(google::protobuf::RpcController* controller, const pb::StoreHeartBeatRequest* request,
                                 pb::StoreHeartBeatResponse* response, google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	RETURN_IF_NOT_INIT(_init_success, response, log_id);
	if (_meta_state_machine != nullptr) {
		_meta_state_machine->store_heartbeat(controller, request, response, done_guard.release());
	}
}

void MetaServer::neo_heartbeat(google::protobuf::RpcController* controller, const pb::BaikalHeartBeatRequest* request,
                               pb::BaikalHeartBeatResponse* response, google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	RETURN_IF_NOT_INIT(_init_success, response, log_id);
	if (_meta_state_machine != nullptr) {
		_meta_state_machine->neo_heartbeat(controller, request, response, done_guard.release());
	}
}

void MetaServer::neo_other_heartbeat(google::protobuf::RpcController* controller,
                                     const pb::BaikalOtherHeartBeatRequest* request,
                                     pb::BaikalOtherHeartBeatResponse* response, google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	RETURN_IF_NOT_INIT(_init_success, response, log_id);
	if (_meta_state_machine != nullptr) {
		_meta_state_machine->neo_other_heartbeat(controller, request, response, done_guard.release());
	}
}

void MetaServer::console_heartbeat(google::protobuf::RpcController* controller,
                                   const pb::ConsoleHeartBeatRequest* request, pb::ConsoleHeartBeatResponse* response,
                                   google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	RETURN_IF_NOT_INIT(_init_success, response, log_id);
	if (_meta_state_machine != nullptr) {
		_meta_state_machine->console_heartbeat(controller, request, response, done_guard.release());
	}
}

void MetaServer::tso_service(google::protobuf::RpcController* controller, const pb::TsoRequest* request,
                             pb::TsoResponse* response, google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	RETURN_IF_NOT_INIT(_init_success, response, log_id);
	if (_tso_state_machine != nullptr) {
		_tso_state_machine->process(controller, request, response, done_guard.release());
	}
}

void MetaServer::migrate(google::protobuf::RpcController* controller, const pb::MigrateRequest* /*request*/,
                         pb::MigrateResponse* response, google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	if (cntl != nullptr) {
		cntl->http_response().set_content_type("text/plain");
		cntl->response_attachment().append("neo-redis meta migrate not supported");
	}
	(void)response;
}

void MetaServer::shutdown_raft() {
	_shutdown = true;
	if (_meta_state_machine != nullptr) {
		_meta_state_machine->shutdown_raft();
	}
	if (_tso_state_machine != nullptr) {
		_tso_state_machine->shutdown_raft();
	}
}

bool MetaServer::have_data() {
	return _meta_state_machine != nullptr && _meta_state_machine->have_data();
}

void MetaServer::close() {
	_shutdown = true;
	_flush_bth.join();
	delete _meta_state_machine;
	_meta_state_machine = nullptr;
	delete _tso_state_machine;
	_tso_state_machine = nullptr;
}

} // namespace neokv
