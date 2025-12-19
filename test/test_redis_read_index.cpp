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
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "common.h"
#include "meta_writer.h"
#include "proto/store.interface.pb.h"
#include "redis_codec.h"
#include "redis_common.h"
#include "redis_router.h"
#include "redis_service.h"
#include "rocks_wrapper.h"
#include "schema_factory.h"
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

class FakeStoreService final : public baikaldb::pb::StoreService {
public:
    std::atomic<bool> ok{true};

    void get_applied_index(google::protobuf::RpcController* /*controller*/,
                          const baikaldb::pb::GetAppliedIndex* request,
                          baikaldb::pb::StoreRes* response,
                          google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->Clear();
        response->set_errmsg("ok");

        if (!request->use_read_idx()) {
            response->set_errcode(baikaldb::pb::SUCCESS);
            response->mutable_region_raft_stat()->set_applied_index(0);
            return;
        }

        if (ok.load()) {
            response->set_errcode(baikaldb::pb::SUCCESS);
            response->mutable_region_raft_stat()->set_applied_index(0);
            return;
        }

        // Simulate leader ReadIndex failure.
        response->set_errcode(baikaldb::pb::NOT_LEADER);
        response->set_leader("127.0.0.1:1");
        response->set_errmsg("not leader");
    }
};

class RedisReadIndexIntegrationTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        // Make test artifacts isolated.
        const int64_t suffix = butil::gettimeofday_us();
        tmp_dir_ = std::string("/tmp/baikaldb_test_redis_read_index_") + std::to_string(suffix);

        // Keep raft artifacts away from repo.
        baikaldb::FLAGS_raftlog_uri = local_uri(tmp_dir_ + "/raftlog_");
        baikaldb::FLAGS_stable_uri = local_uri(tmp_dir_ + "/stable_");
        baikaldb::FLAGS_snapshot_uri = local_uri(tmp_dir_ + "/snapshot");

        // Reduce test latency.
        baikaldb::FLAGS_follow_read_timeout_s = 1;

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
        auto* rocks = baikaldb::RocksWrapper::get_instance();
        ASSERT_EQ(0, rocks->init(tmp_dir_ + "/rocksdb"));
        baikaldb::MetaWriter::get_instance()->init(rocks, rocks->get_meta_info_handle());

        // Init minimal schema so Region::init() can pass.
        auto* factory = baikaldb::SchemaFactory::get_instance();
        factory->init();

        baikaldb::pb::SchemaInfo schema;
        schema.set_namespace_name("__redis__");
        schema.set_database("__redis__");
        schema.set_table_name("kv");
        schema.set_partition_num(1);
        schema.set_namespace_id(1);
        schema.set_database_id(1);
        schema.set_table_id(redis_table_id_);
        schema.set_version(1);

        auto* field = schema.add_fields();
        field->set_field_name("k");
        field->set_field_id(1);
        field->set_mysql_type(baikaldb::pb::STRING);

        auto* pk = schema.add_indexs();
        pk->set_index_type(baikaldb::pb::I_PRIMARY);
        pk->set_index_name("pk");
        // SchemaFactory requires primary index_id == table_id
        pk->set_index_id(redis_table_id_);
        pk->add_field_ids(1);

        factory->update_table(schema);

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

        baikaldb::pb::RegionInfo region_info;
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

        region_ = std::make_shared<baikaldb::Region>(
                rocks,
                factory,
                self_addr_,
                gid,
                pid,
                region_info,
                region_id_,
                false);

        ASSERT_EQ(0, region_->init(true, 0));
        baikaldb::Store::get_instance()->set_region(region_);

        // Seed one key into local RocksDB.
        const std::string user_key = "k1";
        const uint16_t slot = baikaldb::redis_slot(user_key);
        const std::string rocks_key = baikaldb::RedisCodec::encode_key(
                region_id_, baikaldb::REDIS_INDEX_ID, slot, user_key, baikaldb::REDIS_STRING);
        const std::string rocks_val = baikaldb::RedisCodec::encode_value(baikaldb::REDIS_NO_EXPIRE, "v1");
        auto s = rocks->put(rocksdb::WriteOptions(), rocks->get_data_handle(), rocks_key, rocks_val);
        ASSERT_TRUE(s.ok());

        // Start Redis server.
        redis_port_ = pick_free_port();
        ASSERT_GT(redis_port_, 0);
        baikaldb::FLAGS_redis_port = redis_port_;
        baikaldb::FLAGS_redis_advertise_port = redis_port_;

        redis_service_ = std::make_unique<brpc::RedisService>();
        ASSERT_TRUE(baikaldb::register_basic_redis_commands(redis_service_.get()));

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
            baikaldb::Store::get_instance()->erase_region(region_id_);
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
    static inline std::shared_ptr<baikaldb::Region> region_;

    static inline const int64_t region_id_ = 10001;
    static inline const int64_t redis_table_id_ = 1000;
};

TEST_F(RedisReadIndexIntegrationTest, FollowerReadReturnsMovedWhenDisabled) {
    baikaldb::FLAGS_use_read_index = false;

    brpc::RedisResponse res;
    single_call("GET k1", &res);
    const brpc::RedisReply& reply = res.reply(0);
    ASSERT_TRUE(reply.is_error());
    std::string err(reply.error_message());
    ASSERT_NE(std::string::npos, err.find("MOVED"));
}

TEST_F(RedisReadIndexIntegrationTest, FollowerReadReturnsValueWhenEnabled) {
    baikaldb::FLAGS_use_read_index = true;
    fake_store_service_->ok.store(true);

    brpc::RedisResponse res;
    single_call("GET k1", &res);
    const brpc::RedisReply& reply = res.reply(0);
    ASSERT_TRUE(reply.is_string());
    ASSERT_STREQ("v1", reply.c_str());
}

TEST_F(RedisReadIndexIntegrationTest, FollowerReadReturnsMovedWhenReadIndexFails) {
    baikaldb::FLAGS_use_read_index = true;
    baikaldb::FLAGS_demotion_read_index_without_leader = false;
    fake_store_service_->ok.store(false);

    brpc::RedisResponse res;
    single_call("GET k1", &res);
    const brpc::RedisReply& reply = res.reply(0);
    ASSERT_TRUE(reply.is_error());
    std::string err(reply.error_message());
    ASSERT_NE(std::string::npos, err.find("MOVED"));

    // restore default behavior for other tests
    baikaldb::FLAGS_demotion_read_index_without_leader = true;
    fake_store_service_->ok.store(true);
}

} // namespace

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    const int ret = RUN_ALL_TESTS();
    // Avoid potential shutdown-order issues in global destructors (braft/brpc).
    std::_Exit(ret);
}
