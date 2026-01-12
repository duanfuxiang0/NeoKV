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
#include <execinfo.h>
#include <stdio.h>
#include <string>
#include <boost/filesystem.hpp>
#include <memory>
#include "common.h"
#include "my_raft_log.h"
#include "store.h"
#include "qos.h"
#include "memory_profile.h"
#include "rocks_wrapper.h"
#define NEOKV_REDIS_FULL
#include "redis_service.h"
#include "redis_router.h"
#include "redis_ttl_cleaner.h"
#include "redis_common.h"

DEFINE_bool(stop_server_before_core, true, "stop_server_before_core");

brpc::Server server;
brpc::Server redis_server;

// Signal handler for SIGSEGV
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
            DB_FATAL("origin:%s", strings[j]);
            if (name != nullptr) {
                DB_FATAL("%s", name);
            }
        }
    }
    redis_server.Stop(0);
    server.Stop(0);
    sleep(5);
    abort();
}

int main(int argc, char **argv) {
#ifdef NEOKV_REVISION
    google::SetVersionString(NEOKV_REVISION);
    static bvar::Status<std::string> neokv_version("neokv_version", "");
    neokv_version.set_value(NEOKV_REVISION);
#endif
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    srand((unsigned)time(NULL));
    boost::filesystem::path remove_path("init.success");
    boost::filesystem::remove_all(remove_path); 
    
    // Initialize log
    if (neokv::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    
    // Signal handler for SIGSEGV
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

    neokv::register_myraft_extension();
    int ret = 0;
    
    // Initialize store QoS
    neokv::StoreQos* store_qos = neokv::StoreQos::get_instance();
    ret = store_qos->init();
    if (ret < 0) {
        DB_FATAL("store qos init fail");
        return -1;
    }
    
    // Initialize memory handlers
    neokv::MemoryGCHandler::get_instance()->init();
    neokv::MemTrackerPool::get_instance()->init();
    
    // Add Raft service to brpc server
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = neokv::FLAGS_store_port;
    if (0 != braft::add_service(&server, addr)) { 
        DB_FATAL("Fail to init raft");
        return -1;
    }
    DB_WARNING("add raft to brpc server success");
    
    // Initialize Redis server if enabled
    bool redis_started = false;
    std::unique_ptr<brpc::RedisService> redis_service_guard;
    if (neokv::FLAGS_redis_port > 0) {
        // Initialize Redis router (bootstrap mode)
        neokv::RedisRouter::get_instance()->init();

        redis_service_guard.reset(new brpc::RedisService());
        if (!neokv::register_basic_redis_commands(redis_service_guard.get())) {
            DB_FATAL("init redis command handlers fail");
            return -1;
        }
        brpc::ServerOptions redis_options;
        redis_options.redis_service = redis_service_guard.release();
        butil::EndPoint redis_addr;
        redis_addr.ip = butil::IP_ANY;
        redis_addr.port = neokv::FLAGS_redis_port;
        if (redis_server.Start(redis_addr, &redis_options) != 0) {
            DB_FATAL("Fail to start redis server");
            return -1;
        }
        redis_started = true;
        DB_WARNING("start redis server success, listen port: %d", neokv::FLAGS_redis_port);
    } else {
        DB_WARNING("redis_port <= 0, redis service disabled");
    }
    
    // Initialize Store
    neokv::Store* store = neokv::Store::get_instance();
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
    
    // Start the main server
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
    if (redis_started && neokv::FLAGS_redis_ttl_cleanup_interval_s > 0) {
        neokv::RedisTTLCleaner::get_instance()->init(neokv::FLAGS_redis_ttl_cleanup_interval_s);
        DB_WARNING("Redis TTL cleaner started with interval %d seconds", 
                   neokv::FLAGS_redis_ttl_cleanup_interval_s);
    }
    
    std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    DB_WARNING("neo-redis store instance init success");
    
    // Main loop
    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    
    DB_WARNING("receive kill signal, begin to quit");
    
    if (redis_started) {
        // Stop TTL cleaner first
        neokv::RedisTTLCleaner::get_instance()->shutdown();
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
    
    neokv::MemoryGCHandler::get_instance()->close();
    neokv::MemTrackerPool::get_instance()->close();
    
    // Exit if server.join is blocked
    neokv::Bthread bth;
    bth.run([]() {
        bthread_usleep(2 * 60 * 1000 * 1000);
        DB_FATAL("store force exit");
        exit(-1);
    });
    
    server.Stop(0);
    server.Join();
    DB_WARNING("quit success");
    return 0;
}
