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

// Standalone test server for Redis service.
// Usage: ./test_redis_server --redis_port=16379
// Then test with: redis-cli -p 16379 PING

#include <gflags/gflags.h>
#include <brpc/server.h>
#include <brpc/redis.h>
#include "redis_service.h"

DEFINE_int32(redis_port, 16379, "Redis server port for testing");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server redis_server;
    brpc::RedisService* redis_service = new brpc::RedisService();

    if (!baikaldb::register_basic_redis_commands(redis_service)) {
        fprintf(stderr, "Failed to register redis commands\n");
        return -1;
    }

    brpc::ServerOptions options;
    options.redis_service = redis_service;

    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = FLAGS_redis_port;

    if (redis_server.Start(addr, &options) != 0) {
        fprintf(stderr, "Failed to start redis server on port %d\n", FLAGS_redis_port);
        return -1;
    }

    printf("Redis test server started on port %d\n", FLAGS_redis_port);
    printf("Test with:\n");
    printf("  redis-cli -p %d PING\n", FLAGS_redis_port);
    printf("  redis-cli -p %d ECHO \"hello\"\n", FLAGS_redis_port);
    printf("  redis-cli -p %d CLUSTER KEYSLOT \"test\"\n", FLAGS_redis_port);
    printf("Press Ctrl+C to stop.\n");

    redis_server.RunUntilAskedToQuit();
    return 0;
}
