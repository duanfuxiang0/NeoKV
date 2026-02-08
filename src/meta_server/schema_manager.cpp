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

#include "schema_manager.h"

#include <algorithm>
#include <set>
#include <string>
#include <unordered_set>

#include <brpc/controller.h>

#include "cluster_manager.h"
#include "meta_util.h"
#include "region_manager.h"

namespace neokv {

// Keep key names for backward compatibility with existing meta rocksdb data.
const std::string SchemaManager::MAX_NAMESPACE_ID_KEY = "max_namespace_id";
const std::string SchemaManager::MAX_DATABASE_ID_KEY = "max_database_id";
const std::string SchemaManager::MAX_TABLE_ID_KEY = "max_table_id";
const std::string SchemaManager::MAX_REGION_ID_KEY = "max_region_id";

void SchemaManager::process_schema_info(google::protobuf::RpcController* controller,
                                        const pb::MetaManagerRequest* request, pb::MetaManagerResponse* response,
                                        google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
	uint64_t log_id = 0;
	if (cntl != nullptr && cntl->has_log_id()) {
		log_id = cntl->log_id();
	}
	if (_meta_state_machine == nullptr) {
		ERROR_SET_RESPONSE(response, pb::HAVE_NOT_INIT, "meta_state_machine not set", pb::OP_NONE, log_id);
		return;
	}
	if (request == nullptr) {
		ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "null request", pb::OP_NONE, log_id);
		return;
	}
	if (!_meta_state_machine->is_leader()) {
		if (response) {
			response->set_errcode(pb::NOT_LEADER);
			response->set_errmsg("not leader");
			response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
			response->set_op_type(request->op_type());
		}
		DB_WARNING("meta state machine is not leader, request: %s", request->ShortDebugString().c_str());
		return;
	}

	switch (request->op_type()) {
	case pb::OP_UPDATE_REGION: {
		if (!request->has_region_info() && request->region_infos().empty()) {
			ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no region info", request->op_type(), log_id);
			return;
		}
		_meta_state_machine->process(controller, request, response, done_guard.release());
		return;
	}
	case pb::OP_DROP_REGION: {
		if (request->drop_region_ids_size() == 0) {
			ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no drop_region_ids", request->op_type(), log_id);
			return;
		}
		_meta_state_machine->process(controller, request, response, done_guard.release());
		return;
	}
	case pb::OP_SPLIT_REGION: {
		if (!request->has_region_split()) {
			ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no split region info", request->op_type(), log_id);
			return;
		}
		if (response == nullptr) {
			ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "split requires response", request->op_type(), log_id);
			return;
		}
		if (pre_process_for_split_region(request, response, log_id) != 0) {
			return;
		}
		// Apply through raft to allocate new region_id(s) in RegionManager::split_region (on_apply).
		_meta_state_machine->process(controller, request, response, done_guard.release());
		return;
	}
	case pb::OP_MERGE_REGION: {
		if (!request->has_region_merge()) {
			ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no merge region info", request->op_type(), log_id);
			return;
		}
		if (response == nullptr) {
			ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "merge requires response", request->op_type(), log_id);
			return;
		}
		(void)pre_process_for_merge_region(request, response, log_id);
		return;
	}
	default:
		ERROR_SET_RESPONSE(response, pb::UNSUPPORT_REQ_TYPE,
		                   "neo-redis meta only supports region split/merge/update/drop", request->op_type(), log_id);
		return;
	}
}

int SchemaManager::pre_process_for_merge_region(const pb::MetaManagerRequest* request,
                                                pb::MetaManagerResponse* response, uint64_t log_id) {
	TimeCost time_cost;
	const auto& merge = request->region_merge();
	const int64_t src_region_id = merge.src_region_id();
	if (merge.src_end_key().empty()) {
		ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR, "src end key is empty", request->op_type(), log_id);
		return -1;
	}

	auto src_region = RegionManager::get_instance()->get_region_info(src_region_id);
	if (src_region == nullptr) {
		ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR, "can not find src region", request->op_type(), log_id);
		return -1;
	}
	if (src_region->start_key() != merge.src_start_key() || src_region->end_key() != merge.src_end_key()) {
		ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR, "src key diff with local", request->op_type(), log_id);
		return -1;
	}

	const int64_t table_id = merge.table_id();
	const int64_t partition_id = merge.has_partition_id() ? merge.partition_id() : 0;
	SmartRegionInfo best = nullptr;
	std::string best_start;

	// Prefer exact adjacency: dst.start_key == src_end_key.
	RegionManager::get_instance()->traverse_copy_region_map([&](SmartRegionInfo& region) {
		if (region == nullptr) {
			return;
		}
		if (region->table_id() != table_id || region->partition_id() != partition_id) {
			return;
		}
		if (region->deleted()) {
			return;
		}
		const std::string& start_key = region->start_key();
		if (start_key == merge.src_end_key()) {
			best = region;
			best_start = start_key;
			return;
		}
		// Fallback: pick the next region by start_key.
		if (start_key > merge.src_start_key()) {
			if (best == nullptr || (!best_start.empty() && start_key < best_start)) {
				best = region;
				best_start = start_key;
			}
		}
	});

	if (best == nullptr) {
		ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR, "dst merge region not exist", request->op_type(),
		                        log_id);
		return -1;
	}
	auto* out = response->mutable_merge_response();
	out->set_dst_instance(best->leader());
	out->set_dst_start_key(best->start_key());
	out->set_dst_end_key(best->end_key());
	out->set_dst_region_id(best->region_id());
	out->set_version(best->version());
	out->mutable_dst_region()->CopyFrom(*best);

	response->set_errcode(pb::SUCCESS);
	response->set_errmsg("success");
	response->set_op_type(request->op_type());
	DB_WARNING("find dst merge region instance:%s, table_id:%ld, src_region_id:%ld, dst_region_id:%ld, time_cost:%ld, "
	           "log_id:%lu",
	           out->dst_instance().c_str(), table_id, src_region_id, out->dst_region_id(), time_cost.get_time(),
	           log_id);
	return 0;
}

int SchemaManager::pre_process_for_split_region(const pb::MetaManagerRequest* request,
                                                pb::MetaManagerResponse* response, uint64_t log_id) {
	const auto& split = request->region_split();
	const int64_t region_id = split.region_id();
	auto parent_region = RegionManager::get_instance()->get_region_info(region_id);
	if (parent_region == nullptr) {
		ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR, "region not exist", request->op_type(), log_id);
		return -1;
	}

	const int64_t table_id = split.has_table_id() ? split.table_id() : parent_region->table_id();
	(void)table_id; // table_id is kept for proto compatibility (redis doesn't use SQL schema).

	const bool is_tail_split = split.has_tail_split() && split.tail_split();
	int new_region_num = split.has_new_region_num() ? split.new_region_num() : 1;
	if (new_region_num < 1) {
		new_region_num = 1;
	}

	// Replica number comes from existing region meta (generic), fallback to 3.
	int64_t replica_num = parent_region->replica_num();
	if (replica_num <= 0) {
		replica_num = 3;
	}
	// For split bootstrap, cap to 3 peers first; remaining peers (if any) can be added later.
	int64_t select_replica_num = std::min<int64_t>(replica_num, 3);
	if (parent_region->peers_size() < 2 && select_replica_num > 1) {
		ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR, "region not stable, cannot split", request->op_type(),
		                        log_id);
		return -1;
	}

	// Use resource_tag from request if provided; otherwise fall back to empty (no constraint).
	IdcInfo idc(split.has_resource_tag() ? split.resource_tag() : "");

	// Avoid selecting stores already in parent peers when possible.
	std::unordered_set<std::string> parent_peers;
	parent_peers.reserve(parent_region->peers_size());
	for (const auto& peer : parent_region->peers()) {
		parent_peers.insert(peer);
	}

	auto* split_resp = response->mutable_split_response();
	if (!is_tail_split) {
		// Middle split: new region leader is the split source store.
		split_resp->set_new_instance(split.new_instance());
	}
	if (is_tail_split && new_region_num > 1) {
		for (int i = 0; i < new_region_num; ++i) {
			split_resp->add_multi_new_regions();
		}
	}

	auto select_one = [&](std::set<std::string>& exclude, std::string& out) -> int {
		// Try to avoid parent peers up to a few times.
		for (int retry = 0; retry < 3; ++retry) {
			int ret = ClusterManager::get_instance()->select_instance_rolling(idc, exclude, out);
			if (ret < 0) {
				return ret;
			}
			if (parent_peers.count(out) == 0) {
				return 0;
			}
			exclude.insert(out);
		}
		return ClusterManager::get_instance()->select_instance_rolling(idc, exclude, out);
	};

	auto fill_one_region = [&](pb::MultiSplitRegion* multi_out, bool need_set_leader) -> int {
		std::set<std::string> exclude;
		std::string leader_instance;
		if (need_set_leader) {
			int ret = select_one(exclude, leader_instance);
			if (ret < 0) {
				return ret;
			}
			exclude.insert(leader_instance);
			if (multi_out != nullptr) {
				multi_out->set_new_instance(leader_instance);
			} else {
				split_resp->set_new_instance(leader_instance);
			}
		} else {
			// non-tail-split leader already fixed to source instance
			leader_instance = split.new_instance();
			exclude.insert(leader_instance);
		}

		const int64_t need_peers = select_replica_num - 1;
		for (int64_t i = 0; i < need_peers; ++i) {
			std::string peer;
			int ret = select_one(exclude, peer);
			if (ret < 0) {
				return ret;
			}
			exclude.insert(peer);
			if (multi_out != nullptr) {
				multi_out->add_add_peer_instance(peer);
			} else {
				split_resp->add_add_peer_instance(peer);
			}
		}
		return 0;
	};

	if (is_tail_split && new_region_num > 1) {
		for (int i = 0; i < new_region_num; ++i) {
			auto* multi = split_resp->mutable_multi_new_regions(i);
			int ret = fill_one_region(multi, /*need_set_leader*/ true);
			if (ret < 0) {
				ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "select instance fail", request->op_type(), log_id);
				return -1;
			}
		}
	} else {
		const bool need_set_leader = is_tail_split;
		int ret = fill_one_region(nullptr, need_set_leader);
		if (ret < 0) {
			ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "select instance fail", request->op_type(), log_id);
			return -1;
		}
	}

	response->set_errcode(pb::SUCCESS);
	response->set_errmsg("success");
	response->set_op_type(request->op_type());
	return 0;
}

} // namespace neokv
