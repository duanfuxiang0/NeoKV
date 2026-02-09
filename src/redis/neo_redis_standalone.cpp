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

// neo_redis_standalone.cpp
//
// A single-process NeoKV Redis server for integration testing.
// Embeds RocksDB + a single-peer Raft Region (self-elected leader) + Redis service.
// No MetaServer dependency.
//
// Usage:
//   neo_redis_standalone --redis_port=16379 --data_dir=/tmp/neokv_test
//
// The server will:
//   1. Initialize RocksDB in <data_dir>/rocksdb
//   2. Create a single Region covering all 16384 slots
//   3. Start a Raft node (single peer, auto-elects leader)
//   4. Start the Redis service on --redis_port
//   5. Wait for leader election, then serve requests

#include <cstdlib>
#include <ctime>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <memory>
#include <signal.h>

#include <gflags/gflags.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <boost/filesystem.hpp>

#include "common.h"
#include "meta_writer.h"
#include "rocks_wrapper.h"
#include "store.h"
#include "redis_codec.h"
#include "redis_common.h"
#include "redis_router.h"
#define NEOKV_REDIS_FULL
#include "redis_service.h"

// ============================================================================
// Flags
// ============================================================================
DEFINE_string(data_dir, "/tmp/neokv_standalone", "Data directory for RocksDB and Raft logs");
DEFINE_int32(raft_port, 0, "Raft service port (0 = auto-pick free port)");
DEFINE_int32(leader_timeout_ms, 10000, "Timeout waiting for leader election (ms)");

// These are declared in other translation units; we override them.
namespace neokv {
DECLARE_int32(redis_port);
DECLARE_int32(redis_advertise_port);
DECLARE_string(raftlog_uri);
DECLARE_string(stable_uri);
DECLARE_string(snapshot_uri);
DECLARE_int32(store_port);
} // namespace neokv

namespace {

// Constants for the standalone region.
constexpr int64_t kRegionId = 1;
constexpr int64_t kTableId = 1;

brpc::Server g_raft_server;
brpc::Server g_redis_server;
std::shared_ptr<neokv::Region> g_region;
std::unique_ptr<brpc::RedisService> g_redis_service;

volatile bool g_quit = false;

void signal_handler(int /*sig*/) {
	g_quit = true;
}

int pick_free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		return -1;
	}
	sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = htons(0);
	if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
		::close(fd);
		return -1;
	}
	socklen_t len = sizeof(addr);
	if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
		::close(fd);
		return -1;
	}
	int port = ntohs(addr.sin_port);
	::close(fd);
	return port;
}

bool wait_until_leader(int timeout_ms) {
	for (int waited = 0; waited < timeout_ms; waited += 50) {
		if (g_region && g_region->is_leader()) {
			return true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	return false;
}

} // namespace

int main(int argc, char** argv) {
	google::ParseCommandLineFlags(&argc, &argv, true);
	srand(static_cast<unsigned>(time(nullptr)));

	// Suppress glog to stderr for cleaner test output; still log to files.
	if (neokv::init_log(argv[0]) != 0) {
		// If log init fails, continue anyway for testing.
		fprintf(stderr, "WARNING: log init failed, continuing without file logging\n");
	}

	// ========================================================================
	// 1. Prepare directories
	// ========================================================================
	const std::string data_dir = FLAGS_data_dir;
	boost::filesystem::create_directories(data_dir);

	// Override Raft URIs to use local:// (no custom raft log storage needed).
	neokv::FLAGS_raftlog_uri = "local://" + data_dir + "/raftlog_";
	neokv::FLAGS_stable_uri = "local://" + data_dir + "/stable_";
	neokv::FLAGS_snapshot_uri = "local://" + data_dir + "/snapshot";

	// ========================================================================
	// 2. Pick ports
	// ========================================================================
	int raft_port = FLAGS_raft_port;
	if (raft_port <= 0) {
		raft_port = pick_free_port();
		if (raft_port <= 0) {
			fprintf(stderr, "ERROR: failed to pick free port for raft\n");
			return 1;
		}
	}
	neokv::FLAGS_store_port = raft_port;

	if (neokv::FLAGS_redis_port <= 0) {
		neokv::FLAGS_redis_port = 16379;
	}
	if (neokv::FLAGS_redis_advertise_port <= 0) {
		neokv::FLAGS_redis_advertise_port = neokv::FLAGS_redis_port;
	}

	const std::string self_addr = "127.0.0.1:" + std::to_string(raft_port);

	// ========================================================================
	// 3. Initialize RocksDB
	// ========================================================================
	auto* rocks = neokv::RocksWrapper::get_instance();
	if (rocks->init(data_dir + "/rocksdb") != 0) {
		fprintf(stderr, "ERROR: RocksDB init failed\n");
		return 1;
	}
	neokv::MetaWriter::get_instance()->init(rocks, rocks->get_meta_info_handle());

	// ========================================================================
	// 4. Start Raft service
	// ========================================================================
	butil::EndPoint raft_ep;
	raft_ep.ip = butil::IP_ANY;
	raft_ep.port = raft_port;
	if (braft::add_service(&g_raft_server, raft_ep) != 0) {
		fprintf(stderr, "ERROR: failed to add raft service\n");
		return 1;
	}
	if (g_raft_server.Start(raft_ep, nullptr) != 0) {
		fprintf(stderr, "ERROR: failed to start raft server on port %d\n", raft_port);
		return 1;
	}

	// ========================================================================
	// 5. Create a single-peer Region covering all slots
	// ========================================================================
	neokv::pb::RegionInfo region_info;
	region_info.set_region_id(kRegionId);
	region_info.set_table_id(kTableId);
	region_info.set_version(1);
	region_info.set_partition_id(0);
	region_info.set_replica_num(1); // Single peer -> auto-elects leader
	region_info.clear_start_key();  // Cover all slots
	region_info.clear_end_key();
	region_info.add_peers(self_addr);

	std::string group_name = "region_" + std::to_string(kRegionId);
	braft::GroupId gid(group_name);
	butil::EndPoint self_ep;
	butil::str2endpoint("127.0.0.1", raft_port, &self_ep);
	braft::PeerId pid(self_ep);

	g_region = std::make_shared<neokv::Region>(rocks, self_addr, gid, pid, region_info, kRegionId, false);
	if (g_region->init(true, 0) != 0) {
		fprintf(stderr, "ERROR: Region init failed\n");
		return 1;
	}

	// Register region in the Store singleton.
	neokv::Store::get_instance()->set_region(g_region);

	// Initialize Redis router and build slot table.
	neokv::RedisRouter::get_instance()->init();
	neokv::RedisRouter::get_instance()->rebuild_slot_table();

	// ========================================================================
	// 6. Start Redis service
	// ========================================================================
	g_redis_service.reset(new brpc::RedisService());
	if (!neokv::register_basic_redis_commands(g_redis_service.get())) {
		fprintf(stderr, "ERROR: failed to register redis commands\n");
		return 1;
	}

	brpc::ServerOptions redis_options;
	redis_options.redis_service = g_redis_service.release();
	butil::EndPoint redis_ep;
	redis_ep.ip = butil::IP_ANY;
	redis_ep.port = neokv::FLAGS_redis_port;
	if (g_redis_server.Start(redis_ep, &redis_options) != 0) {
		fprintf(stderr, "ERROR: failed to start redis server on port %d\n", neokv::FLAGS_redis_port);
		return 1;
	}

	// ========================================================================
	// 7. Wait for leader election
	// ========================================================================
	if (!wait_until_leader(FLAGS_leader_timeout_ms)) {
		fprintf(stderr, "ERROR: leader election timed out after %d ms\n", FLAGS_leader_timeout_ms);
		return 1;
	}

	// Print ready message for test harness to detect.
	fprintf(stdout, "NEOKV_READY port=%d\n", neokv::FLAGS_redis_port);
	fflush(stdout);

	// ========================================================================
	// 8. Main loop
	// ========================================================================
	signal(SIGTERM, signal_handler);
	signal(SIGINT, signal_handler);

	while (!g_quit) {
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}

	// ========================================================================
	// 9. Shutdown
	// ========================================================================
	fprintf(stdout, "Shutting down...\n");
	fflush(stdout);

	g_redis_server.Stop(0);
	g_redis_server.Join();

	if (g_region) {
		neokv::Store::get_instance()->erase_region(kRegionId);
		g_region->shutdown();
		g_region->join();
		g_region.reset();
	}

	g_raft_server.Stop(0);
	g_raft_server.Join();

	// Use _Exit to avoid potential shutdown-order issues in global destructors (braft/brpc).
	std::_Exit(0);
}
