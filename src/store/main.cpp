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

#include <ctime>
#include <cstdlib>
#include <net/if.h>
#include <sys/ioctl.h>
#include <gflags/gflags.h>
#include <signal.h>
#include <cxxabi.h>
#include<execinfo.h>
#include <stdio.h>
#include <string>
#include <boost/filesystem.hpp>
//#include <gperftools/malloc_extension.h>
#include <memory>
#include "common.h"
#include "my_raft_log.h"
#include "store.h"
#include "reverse_common.h"
#include "fn_manager.h"
#include "schema_factory.h"
#include "qos.h"
#include "memory_profile.h"
#include "arrow_function.h"
#include "arrow_exec_node.h"
#include "compaction_server.h"
#include "rocks_wrapper.h"
#include "schema_factory.h"
#define BAIKALDB_REDIS_FULL
#include "redis_service.h"
#include "redis_router.h"
#include "redis_ttl_cleaner.h"
#include "redis_common.h"

namespace baikaldb {
DECLARE_bool(use_fulltext_wordweight_segment);
DECLARE_bool(use_fulltext_wordseg_wordrank_segment);
DEFINE_string(wordrank_conf, "./config/drpc_client.xml", "wordrank conf path");
} // namespace baikaldb
DEFINE_bool(stop_server_before_core, true, "stop_server_before_core");
DEFINE_int32(compaction_sst_cache_capacity, 200000, "compaction_sst_cache_capacity");

brpc::Server server;
brpc::Server redis_server;
// 内存过大时，coredump需要几分钟，这期间会丢请求
// 理论上应该采用可重入函数，但是堆栈不好获取
// 考虑到core的概率不大，先这样处理
void sigsegv_handler(int signum, siginfo_t* info, void* ptr) {
    void* buffer[1000];
    char** strings;
    int nptrs = backtrace(buffer, 1000);
    DB_FATAL("segment fault, backtrace() returned %d addresses", nptrs);
    strings = backtrace_symbols(buffer, nptrs);
    if (strings != NULL) {
        for (int j = 0; j < nptrs; j++) {
            int status = 0;
            char* name = abi::__cxa_demangle(strings[j], nullptr, nullptr, &status);
            DB_FATAL("orgin:%s", strings[j]);
            if (name != nullptr) {
                DB_FATAL("%s", name);
            }
        }
    }
    redis_server.Stop(0);
    server.Stop(0);
    // core的过程中依然会hang住baikaldb请求
    // 先等一会，baikaldb反应过来
    // 后续再调整
    sleep(5);
    abort();
}

int main(int argc, char **argv) {
#ifdef BAIKALDB_REVISION
    google::SetVersionString(BAIKALDB_REVISION);
    static bvar::Status<std::string> baikaldb_version("baikaldb_version", "");
    baikaldb_version.set_value(BAIKALDB_REVISION);
#endif
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    srand((unsigned)time(NULL));
    boost::filesystem::path remove_path("init.success");
    boost::filesystem::remove_all(remove_path); 
    // Initail log
    if (baikaldb::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    // 信号处理函数非可重入，可能会死锁
    if (FLAGS_stop_server_before_core) {
        struct sigaction act;
        int sig = SIGSEGV;
        sigemptyset(&act.sa_mask);
        act.sa_sigaction = sigsegv_handler;
        act.sa_flags = SA_SIGINFO;
        if (sigaction(sig, &act, NULL) < 0) {
            DB_FATAL("sigaction fail, %m");
            exit(1);
        }
    }
//    DB_WARNING("log file load success; GetMemoryReleaseRate:%f", 
//            MallocExtension::instance()->GetMemoryReleaseRate());
    baikaldb::register_myraft_extension();
    int ret = 0;
    baikaldb::Tokenizer::get_instance()->init();
#ifdef BAIDU_INTERNAL
    //init wordrank_client
    ret = ::drpc::init_env(baikaldb::FLAGS_wordrank_conf);
    if (ret < 0) {
        DB_WARNING("wordrank init_env failed");
        return -1;
    }
    if (baikaldb::FLAGS_use_fulltext_wordseg_wordrank_segment) {
        baikaldb::wordrank_client = new drpc::NLPCClient();
        baikaldb::wordseg_client = new drpc::NLPCClient();
        if (baikaldb::wordrank_client->init("nlpc_wordrank_208") != 0) {
            DB_WARNING("init wordrank agent failed");
            return -1;
        }

        if (baikaldb::wordseg_client->init("nlpc_wordseg_3016") != 0) {
            DB_WARNING("init wordseg agent failed");
            return -1;
        }
    }

    if (baikaldb::FLAGS_use_fulltext_wordweight_segment) {
        baikaldb::wordweight_client = new drpc::NLPCClient();
        if (baikaldb::wordweight_client->init("nlpc_wordweight_1121") != 0) {
            DB_WARNING("init wordweight agent failed");
            return -1;
        }
    }

    DB_WARNING("init nlpc success");
#endif
    /* 
    auto call = []() {
        std::ifstream extra_fs("test_file");
        std::string word((std::istreambuf_iterator<char>(extra_fs)),
                std::istreambuf_iterator<char>());
        baikaldb::TimeCost tt1;
        for (int i = 0; i < 1000000000; i++) {
            //word+="1";
            std::string word2 = word + "1";
            std::map<std::string, float> term_map;
            baikaldb::Tokenizer::get_instance()->wordrank(word2, term_map);
            if (i%1000==0) {
                DB_WARNING("wordrank:%d",i);
            }
        }
        DB_WARNING("wordrank:%ld", tt1.get_time());
    };
    baikaldb::ConcurrencyBthread cb(100);
    for (int i = 0; i < 100; i++) {
        cb.run(call);
    }
    cb.join();
    return 0;
    */
    // init singleton
    baikaldb::FunctionManager::instance()->init();
    if (baikaldb::SchemaFactory::get_instance()->init() != 0) {
        DB_FATAL("SchemaFactory init failed");
        return -1;
    }
    if (baikaldb::ArrowFunctionManager::instance()->RegisterAllArrowFunction() != 0) {
        DB_FATAL("ArrowFunctionManager init failed");
        return -1;
    }
    if (baikaldb::ArrowExecNodeManager::RegisterAllArrowExecNode() != 0) {
        DB_FATAL("RegisterAllArrowExecNode failed");
        return -1;
    }
    if (baikaldb::GlobalArrowExecutor::init() != 0) {
        DB_FATAL("GlobalArrowExecutor init failed");
        return -1;
    }
    baikaldb::CompactionSstCache::get_instance()->init(FLAGS_compaction_sst_cache_capacity);
    //add service
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = baikaldb::FLAGS_store_port;
    //将raft加入到baidu-rpc server中
    if (0 != braft::add_service(&server, addr)) { 
        DB_FATAL("Fail to init raft");
        return -1;
    }
    DB_WARNING("add raft to baidu-rpc server success");
    baikaldb::StoreQos* store_qos = baikaldb::StoreQos::get_instance();
    ret = store_qos->init();
    if (ret < 0) {
        DB_FATAL("store qos init fail");
        return -1;
    }
    baikaldb::MemoryGCHandler::get_instance()->init();
    baikaldb::MemTrackerPool::get_instance()->init();
    bool redis_started = false;
    std::unique_ptr<brpc::RedisService> redis_service_guard;
    if (baikaldb::FLAGS_redis_port > 0) {
        // Register Redis index to SchemaFactory (no SQL table needed)
        // This allows SplitCompactionFilter to correctly handle Redis keys
        baikaldb::SchemaFactory::get_instance()->register_redis_index(baikaldb::REDIS_INDEX_ID);
        DB_WARNING("Registered Redis index_id=%ld for SplitCompactionFilter", baikaldb::REDIS_INDEX_ID);
        
        // Initialize Redis router (bootstrap mode - will re-init after store is ready)
        baikaldb::RedisRouter::get_instance()->init();

        redis_service_guard.reset(new brpc::RedisService());
        if (!baikaldb::register_basic_redis_commands(redis_service_guard.get())) {
            DB_FATAL("init redis command handlers fail");
            return -1;
        }
        brpc::ServerOptions redis_options;
        redis_options.redis_service = redis_service_guard.release();
        butil::EndPoint redis_addr;
        redis_addr.ip = butil::IP_ANY;
        redis_addr.port = baikaldb::FLAGS_redis_port;
        if (redis_server.Start(redis_addr, &redis_options) != 0) {
            DB_FATAL("Fail to start redis server");
            return -1;
        }
        redis_started = true;
        DB_WARNING("start redis server success, listen port: %d", baikaldb::FLAGS_redis_port);
    } else {
        DB_WARNING("redis_port <= 0, redis service disabled");
    }
    //注册处理Store逻辑的service服务
    baikaldb::Store* store = baikaldb::Store::get_instance();
    std::vector<std::int64_t> init_region_ids;
    ret = store->init_before_listen(init_region_ids);
    if (ret < 0) {
        DB_FATAL("Store instance init_before_listen fail");
        return -1;
    } 
    if (0 != server.AddService(store, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add StoreService");
        return -1;
    }
    baikaldb::CompactionServer* compaction_server = baikaldb::CompactionServer::get_instance();
    if (0 != server.AddService(compaction_server, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add StoreService");
        return -1;
    }
    if (server.Start(addr, NULL) != 0) {
        DB_FATAL("Fail to start server");
        return -1;
    }
    DB_WARNING("start rpc success");
    ret = store->init_after_listen(init_region_ids);
    if (ret < 0) {
        DB_FATAL("Store instance init_after_listen fail");
        return -1;
    }
    
    // Initialize Redis TTL cleaner if Redis is enabled
    if (redis_started && baikaldb::FLAGS_redis_ttl_cleanup_interval_s > 0) {
        baikaldb::RedisTTLCleaner::get_instance()->init(baikaldb::FLAGS_redis_ttl_cleanup_interval_s);
        DB_WARNING("Redis TTL cleaner started with interval %d seconds", 
                   baikaldb::FLAGS_redis_ttl_cleanup_interval_s);
    }
    
    std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    DB_WARNING("store instance init success");
    //server.RunUntilAskedToQuit();
    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    DB_WARNING("recevie kill signal, begin to quit");
    if (redis_started) {
        // Stop TTL cleaner first
        baikaldb::RedisTTLCleaner::get_instance()->shutdown();
        DB_WARNING("redis TTL cleaner stopped");
        
        redis_server.Stop(0);
        redis_server.Join();
        DB_WARNING("redis server quit success");
    }
    store->shutdown_raft();
    store->close();
    DB_WARNING("store close success");
    store_qos->close();
    DB_WARNING("store qos close success");
    baikaldb::MemoryGCHandler::get_instance()->close();
    baikaldb::MemTrackerPool::get_instance()->close();
    // exit if server.join is blocked
    baikaldb::Bthread bth;
    bth.run([]() {
            bthread_usleep(2 * 60 * 1000 * 1000);
            DB_FATAL("store forse exit");
            exit(-1);
        });
    // 需要后关端口
    server.Stop(0);
    server.Join();
    DB_WARNING("quit success");
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
