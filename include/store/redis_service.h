#pragma once

#include <cstdint>
#include <string>
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/redis.h>
#else
#include <brpc/redis.h>
#endif

namespace baikaldb {

// Calculate redis CRC16 (poly 0x1021, init 0) for hash-tag usage.
uint16_t redis_crc16(const std::string& input);

// Compute cluster slot for given key with {tag} semantics.
uint16_t redis_slot(const std::string& key);

// Register minimal Redis commands (PING/ECHO/CLUSTER KEYSLOT).
bool register_basic_redis_commands(brpc::RedisService* service);

} // namespace baikaldb
