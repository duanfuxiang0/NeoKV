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

#include <gtest/gtest.h>

#include <brpc/channel.h>
#include <brpc/redis.h>
#include <brpc/server.h>

#include <boost/filesystem.hpp>
#include <braft/raft.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "meta_writer.h"
#include "proto/store.interface.pb.h"
#include "redis_codec.h"
#include "redis_common.h"
#include "redis_router.h"
#include "redis_service.h"
#include "rocks_wrapper.h"
#include "store.h"

namespace {

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

std::string local_uri(const std::string& path) {
	return std::string("local://") + path;
}

class FakeStoreService final : public neokv::pb::StoreService {
public:
	std::atomic<bool> ok {true};

	void get_applied_index(google::protobuf::RpcController* /*controller*/, const neokv::pb::GetAppliedIndex* request,
	                       neokv::pb::StoreRes* response, google::protobuf::Closure* done) override {
		brpc::ClosureGuard guard(done);
		response->Clear();
		response->set_errmsg("ok");

		if (!request->use_read_idx()) {
			response->set_errcode(neokv::pb::SUCCESS);
			response->mutable_region_raft_stat()->set_applied_index(0);
			return;
		}

		if (ok.load()) {
			response->set_errcode(neokv::pb::SUCCESS);
			response->mutable_region_raft_stat()->set_applied_index(0);
			return;
		}

		// Simulate leader ReadIndex failure.
		response->set_errcode(neokv::pb::NOT_LEADER);
		response->set_leader("127.0.0.1:1");
		response->set_errmsg("not leader");
	}
};

class RedisReadIndexIntegrationTest : public ::testing::Test {
protected:
	static void SetUpTestSuite() {
		// Make test artifacts isolated.
		const int64_t suffix = butil::gettimeofday_us();
		tmp_dir_ = std::string("/tmp/neokv_test_redis_read_index_") + std::to_string(suffix);

		// Keep raft artifacts away from repo.
		neokv::FLAGS_raftlog_uri = local_uri(tmp_dir_ + "/raftlog_");
		neokv::FLAGS_stable_uri = local_uri(tmp_dir_ + "/stable_");
		neokv::FLAGS_snapshot_uri = local_uri(tmp_dir_ + "/snapshot");

		// Reduce test latency.
		neokv::FLAGS_follow_read_timeout_s = 1;

		// Start fake StoreService (leader) for get_applied_index.
		leader_port_ = pick_free_port();
		ASSERT_GT(leader_port_, 0);
		leader_addr_ = std::string("127.0.0.1:") + std::to_string(leader_port_);

		fake_store_service_ = std::make_unique<FakeStoreService>();
		ASSERT_EQ(0, store_server_.AddService(fake_store_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE));
		butil::EndPoint store_ep;
		ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", leader_port_, &store_ep));
		ASSERT_EQ(0, store_server_.Start(store_ep, nullptr));

		ASSERT_TRUE(boost::filesystem::create_directories(tmp_dir_));

		// Init RocksDB (data/meta/raft log are all in RocksWrapper DBs).
		auto* rocks = neokv::RocksWrapper::get_instance();
		ASSERT_EQ(0, rocks->init(tmp_dir_ + "/rocksdb"));
		neokv::MetaWriter::get_instance()->init(rocks, rocks->get_meta_info_handle());

		// Neo-redis: Region init does not depend on SQL schema.

		// Create a follower region (2 peers, cannot elect leader alone).
		self_port_ = pick_free_port();
		ASSERT_GT(self_port_, 0);
		self_addr_ = std::string("127.0.0.1:") + std::to_string(self_port_);

		// braft Node requires an RPC server attached to the peer endpoint.
		butil::EndPoint raft_ep;
		raft_ep.ip = butil::IP_ANY;
		raft_ep.port = self_port_;
		ASSERT_EQ(0, braft::add_service(&raft_server_, raft_ep));
		ASSERT_EQ(0, raft_server_.Start(raft_ep, nullptr));

		neokv::pb::RegionInfo region_info;
		region_info.set_region_id(region_id_);
		region_info.set_table_id(redis_table_id_);
		region_info.set_version(1);
		region_info.set_partition_id(0);
		region_info.set_replica_num(2);
		// Cover all slots.
		region_info.clear_start_key();
		region_info.clear_end_key();
		region_info.add_peers(leader_addr_);
		region_info.add_peers(self_addr_);

		braft::GroupId gid("redis_read_index_test_group");
		butil::EndPoint self_ep;
		ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", self_port_, &self_ep));
		braft::PeerId pid(self_ep);

		region_ = std::make_shared<neokv::Region>(rocks, self_addr_, gid, pid, region_info, region_id_, false);

		ASSERT_EQ(0, region_->init(true, 0));
		neokv::Store::get_instance()->set_region(region_);
		neokv::RedisRouter::get_instance()->init();

		// Seed one key into local RocksDB.
		const std::string user_key = "k1";
		const uint16_t slot = neokv::redis_slot(user_key);
		const std::string rocks_key =
		    neokv::RedisCodec::encode_key(region_id_, redis_table_id_, slot, user_key, neokv::REDIS_STRING);
		const std::string rocks_val = neokv::RedisCodec::encode_value(neokv::REDIS_NO_EXPIRE, "v1");
		auto s = rocks->put(rocksdb::WriteOptions(), rocks->get_data_handle(), rocks_key, rocks_val);
		ASSERT_TRUE(s.ok());

		// Start Redis server.
		redis_port_ = pick_free_port();
		ASSERT_GT(redis_port_, 0);
		neokv::FLAGS_redis_port = redis_port_;
		neokv::FLAGS_redis_advertise_port = redis_port_;

		redis_service_ = std::make_unique<brpc::RedisService>();
		ASSERT_TRUE(neokv::register_basic_redis_commands(redis_service_.get()));

		brpc::ServerOptions opts;
		opts.redis_service = redis_service_.get();

		butil::EndPoint redis_ep;
		ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", redis_port_, &redis_ep));
		ASSERT_EQ(0, redis_server_.Start(redis_ep, &opts));

		// Client channel to Redis server.
		brpc::ChannelOptions ch_opts;
		ch_opts.protocol = brpc::PROTOCOL_REDIS;
		ch_opts.timeout_ms = 1000;
		ASSERT_EQ(0, redis_channel_.Init((std::string("127.0.0.1:") + std::to_string(redis_port_)).c_str(), &ch_opts));
	}

	static void TearDownTestSuite() {
		if (region_) {
			neokv::Store::get_instance()->erase_region(region_id_);
			region_->shutdown();
			region_->join();
			region_.reset();
		}
		redis_server_.Stop(0);
		redis_server_.Join();
		raft_server_.Stop(0);
		raft_server_.Join();
		store_server_.Stop(0);
		store_server_.Join();
		redis_service_.reset();
		fake_store_service_.reset();
	}

	static void single_call(const std::string& cmd, brpc::RedisResponse* out) {
		brpc::RedisRequest req;
		brpc::RedisResponse res;
		brpc::Controller cntl;
		req.AddCommand(cmd);
		redis_channel_.CallMethod(nullptr, &cntl, &req, &res, nullptr);
		EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
		EXPECT_EQ(1, res.reply_size());
		if (out != nullptr) {
			out->Swap(&res);
		}
	}

	static std::shared_ptr<neokv::Region> create_single_peer_region(int64_t region_id, int64_t table_id) {
		auto* rocks = neokv::RocksWrapper::get_instance();
		if (rocks == nullptr) {
			return nullptr;
		}

		neokv::pb::RegionInfo region_info;
		region_info.set_region_id(region_id);
		region_info.set_table_id(table_id);
		region_info.set_version(1);
		region_info.set_partition_id(0);
		region_info.set_replica_num(1);
		region_info.clear_start_key();
		region_info.clear_end_key();
		region_info.add_peers(self_addr_);

		std::string group_name = "redis_set_cond_group_" + std::to_string(region_id);
		braft::GroupId gid(group_name);
		butil::EndPoint self_ep;
		if (butil::str2endpoint("127.0.0.1", self_port_, &self_ep) != 0) {
			return nullptr;
		}
		braft::PeerId pid(self_ep);

		auto region = std::make_shared<neokv::Region>(rocks, self_addr_, gid, pid, region_info, region_id, false);
		if (region->init(true, 0) != 0) {
			return nullptr;
		}
		return region;
	}

	static bool wait_until_leader(const std::shared_ptr<neokv::Region>& region, int timeout_ms) {
		for (int waited = 0; waited < timeout_ms; waited += 50) {
			if (region != nullptr && region->is_leader()) {
				return true;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		}
		return false;
	}

	static inline std::string tmp_dir_;
	static inline int leader_port_ = 0;
	static inline std::string leader_addr_;
	static inline int redis_port_ = 0;
	static inline int self_port_ = 0;
	static inline std::string self_addr_;

	static inline brpc::Server store_server_;
	static inline brpc::Server redis_server_;
	static inline brpc::Server raft_server_;
	static inline std::unique_ptr<FakeStoreService> fake_store_service_;
	static inline std::unique_ptr<brpc::RedisService> redis_service_;

	static inline brpc::Channel redis_channel_;
	static inline std::shared_ptr<neokv::Region> region_;

	static inline const int64_t region_id_ = 10001;
	static inline const int64_t redis_table_id_ = 1000;
};

TEST_F(RedisReadIndexIntegrationTest, FollowerReadReturnsMovedWhenDisabled) {
	neokv::FLAGS_use_read_index = false;

	brpc::RedisResponse res;
	single_call("GET k1", &res);
	const brpc::RedisReply& reply = res.reply(0);
	ASSERT_TRUE(reply.is_error());
	std::string err(reply.error_message());
	ASSERT_NE(std::string::npos, err.find("MOVED"));
}

TEST_F(RedisReadIndexIntegrationTest, FollowerReadReturnsValueWhenEnabled) {
	neokv::FLAGS_use_read_index = true;
	fake_store_service_->ok.store(true);

	brpc::RedisResponse res;
	single_call("GET k1", &res);
	const brpc::RedisReply& reply = res.reply(0);
	ASSERT_TRUE(reply.is_string());
	ASSERT_STREQ("v1", reply.c_str());
}

TEST_F(RedisReadIndexIntegrationTest, FollowerReadReturnsMovedWhenReadIndexFails) {
	neokv::FLAGS_use_read_index = true;
	neokv::FLAGS_demotion_read_index_without_leader = false;
	fake_store_service_->ok.store(false);

	brpc::RedisResponse res;
	single_call("GET k1", &res);
	const brpc::RedisReply& reply = res.reply(0);
	ASSERT_TRUE(reply.is_error());
	std::string err(reply.error_message());
	ASSERT_NE(std::string::npos, err.find("MOVED"));

	// restore default behavior for other tests
	neokv::FLAGS_demotion_read_index_without_leader = true;
	fake_store_service_->ok.store(true);
}

TEST_F(RedisReadIndexIntegrationTest, ReturnsClusterDownWhenSlotNotServed) {
	auto saved_region = region_;
	neokv::Store::get_instance()->erase_region(region_id_);

	brpc::RedisResponse res;
	single_call("GET k1", &res);
	const brpc::RedisReply& reply = res.reply(0);
	ASSERT_TRUE(reply.is_error());
	std::string err(reply.error_message());
	ASSERT_NE(std::string::npos, err.find("CLUSTERDOWN"));

	auto restore = saved_region;
	neokv::Store::get_instance()->set_region(restore);
}

TEST_F(RedisReadIndexIntegrationTest, SetSyntaxValidationRunsBeforeRouting) {
	brpc::RedisResponse res_missing_ttl;
	single_call("SET k1 v1 EX", &res_missing_ttl);
	const brpc::RedisReply& reply_missing_ttl = res_missing_ttl.reply(0);
	ASSERT_TRUE(reply_missing_ttl.is_error());
	std::string err_missing_ttl(reply_missing_ttl.error_message());
	ASSERT_NE(std::string::npos, err_missing_ttl.find("ERR syntax error"));

	brpc::RedisResponse res_unknown_opt;
	single_call("SET k1 v1 FOO", &res_unknown_opt);
	const brpc::RedisReply& reply_unknown_opt = res_unknown_opt.reply(0);
	ASSERT_TRUE(reply_unknown_opt.is_error());
	std::string err_unknown_opt(reply_unknown_opt.error_message());
	ASSERT_NE(std::string::npos, err_unknown_opt.find("ERR syntax error"));
}

TEST_F(RedisReadIndexIntegrationTest, SetNxXxConditionsAppliedByRaft) {
	auto saved_region = region_;
	std::shared_ptr<neokv::Region> leader_region;
	const int64_t leader_region_id = region_id_ + 100;

	neokv::Store::get_instance()->erase_region(region_id_);
	ON_SCOPE_EXIT([&]() {
		neokv::Store::get_instance()->erase_region(leader_region_id);
		if (leader_region != nullptr) {
			leader_region->shutdown();
			leader_region->join();
		}
		neokv::Store::get_instance()->set_region(saved_region);
	});

	leader_region = create_single_peer_region(leader_region_id, redis_table_id_);
	ASSERT_TRUE(leader_region != nullptr);
	neokv::Store::get_instance()->set_region(leader_region);
	ASSERT_TRUE(wait_until_leader(leader_region, 5000));

	brpc::RedisResponse res_xx_missing;
	single_call("SET nx_xx_key v0 XX", &res_xx_missing);
	const brpc::RedisReply& reply_xx_missing = res_xx_missing.reply(0);
	ASSERT_TRUE(reply_xx_missing.is_nil());

	brpc::RedisResponse res_nx_ok;
	single_call("SET nx_xx_key v1 NX", &res_nx_ok);
	const brpc::RedisReply& reply_nx_ok = res_nx_ok.reply(0);
	ASSERT_TRUE(reply_nx_ok.is_string());
	ASSERT_STREQ("OK", reply_nx_ok.c_str());

	brpc::RedisResponse res_nx_again;
	single_call("SET nx_xx_key v2 NX", &res_nx_again);
	const brpc::RedisReply& reply_nx_again = res_nx_again.reply(0);
	ASSERT_TRUE(reply_nx_again.is_nil());

	brpc::RedisResponse res_xx_ok;
	single_call("SET nx_xx_key v3 XX", &res_xx_ok);
	const brpc::RedisReply& reply_xx_ok = res_xx_ok.reply(0);
	ASSERT_TRUE(reply_xx_ok.is_string());
	ASSERT_STREQ("OK", reply_xx_ok.c_str());

	brpc::RedisResponse res_get;
	single_call("GET nx_xx_key", &res_get);
	const brpc::RedisReply& reply_get = res_get.reply(0);
	ASSERT_TRUE(reply_get.is_string());
	ASSERT_STREQ("v3", reply_get.c_str());
}

TEST_F(RedisReadIndexIntegrationTest, SetNxConcurrentOnlyOneSucceeds) {
	auto saved_region = region_;
	std::shared_ptr<neokv::Region> leader_region;
	const int64_t leader_region_id = region_id_ + 101;

	neokv::Store::get_instance()->erase_region(region_id_);
	ON_SCOPE_EXIT([&]() {
		neokv::Store::get_instance()->erase_region(leader_region_id);
		if (leader_region != nullptr) {
			leader_region->shutdown();
			leader_region->join();
		}
		neokv::Store::get_instance()->set_region(saved_region);
	});

	leader_region = create_single_peer_region(leader_region_id, redis_table_id_);
	ASSERT_TRUE(leader_region != nullptr);
	neokv::Store::get_instance()->set_region(leader_region);
	ASSERT_TRUE(wait_until_leader(leader_region, 5000));

	brpc::RedisResponse res_del;
	single_call("DEL nx_race_key", &res_del);

	constexpr int kThreads = 16;
	std::atomic<int> ok_count {0};
	std::atomic<int> nil_count {0};
	std::atomic<int> error_count {0};

	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (int i = 0; i < kThreads; ++i) {
		threads.emplace_back([i, &ok_count, &nil_count, &error_count]() {
			brpc::RedisRequest req;
			brpc::RedisResponse res;
			brpc::Controller cntl;
			std::string cmd = "SET nx_race_key v_" + std::to_string(i) + " NX";
			req.AddCommand(cmd);
			redis_channel_.CallMethod(nullptr, &cntl, &req, &res, nullptr);
			if (cntl.Failed() || res.reply_size() != 1) {
				++error_count;
				return;
			}
			const brpc::RedisReply& reply = res.reply(0);
			if (reply.is_error()) {
				++error_count;
			} else if (reply.is_nil()) {
				++nil_count;
			} else if (reply.is_string() && std::string(reply.c_str()) == "OK") {
				++ok_count;
			} else {
				++error_count;
			}
		});
	}
	for (auto& th : threads) {
		th.join();
	}

	ASSERT_EQ(1, ok_count.load());
	ASSERT_EQ(kThreads - 1, nil_count.load());
	ASSERT_EQ(0, error_count.load());

	brpc::RedisResponse res_get;
	single_call("GET nx_race_key", &res_get);
	const brpc::RedisReply& reply_get = res_get.reply(0);
	ASSERT_TRUE(reply_get.is_string());
	std::string final_value(reply_get.c_str());
	ASSERT_NE(std::string::npos, final_value.find("v_"));
}

} // namespace

int main(int argc, char** argv) {
	testing::InitGoogleTest(&argc, argv);
	const int ret = RUN_ALL_TESTS();
	// Avoid potential shutdown-order issues in global destructors (braft/brpc).
	std::_Exit(ret);
}
