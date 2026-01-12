// Enable full NEOKV integration for Redis commands
#define NEOKV_REDIS_FULL

#include "redis_service.h"
#include "redis_codec.h"
#include "redis_router.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

// Conditional includes for full NEOKV integration
#ifdef NEOKV_REDIS_FULL
#include "store.h"
#include "region.h"
#include "rocks_wrapper.h"
#include "common.h"
#include "key_encoder.h"
#include "redis_common.h"
#include <sstream>
#include <set>
#endif

// Fallback logging for standalone compilation
#ifndef DB_FATAL
#define DB_FATAL(fmt, ...) fprintf(stderr, "[ERROR] " fmt "\n", ##__VA_ARGS__)
#endif
#ifndef DB_WARNING
#define DB_WARNING(fmt, ...) fprintf(stderr, "[WARN] " fmt "\n", ##__VA_ARGS__)
#endif

namespace {

std::string extract_hash_tag(const std::string& key) {
    auto left = key.find('{');
    if (left == std::string::npos) {
        return key;
    }
    auto right = key.find('}', left + 1);
    if (right != std::string::npos && right > left + 1) {
        return key.substr(left + 1, right - left - 1);
    }
    return key;
}

bool equals_ignore_case(const butil::StringPiece& lhs, const char* rhs) {
    const size_t rhs_len = strlen(rhs);
    if (lhs.size() != rhs_len) {
        return false;
    }
    for (size_t i = 0; i < rhs_len; ++i) {
        if (std::tolower(lhs[i]) != std::tolower(rhs[i])) {
            return false;
        }
    }
    return true;
}

// ============================================================================
// Phase 0 Command Handlers (PING/ECHO/CLUSTER KEYSLOT)
// ============================================================================

class PingCommandHandler final : public brpc::RedisCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        if (args.size() == 1) {
            output->SetStatus("PONG");
            return brpc::REDIS_CMD_HANDLED;
        }
        if (args.size() == 2) {
            output->SetString(args[1]);
            return brpc::REDIS_CMD_HANDLED;
        }
        output->SetError("ERR wrong number of arguments for 'ping' command");
        return brpc::REDIS_CMD_HANDLED;
    }
};

class EchoCommandHandler final : public brpc::RedisCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        if (args.size() != 2) {
            output->SetError("ERR wrong number of arguments for 'echo' command");
            return brpc::REDIS_CMD_HANDLED;
        }
        output->SetString(args[1]);
        return brpc::REDIS_CMD_HANDLED;
    }
};

// Get the advertise address for Redis redirects
std::string get_redis_advertise_addr() {
#ifdef NEOKV_REDIS_FULL
    std::string ip = neokv::FLAGS_redis_advertise_ip;
    int port = neokv::FLAGS_redis_advertise_port;
    
    if (ip.empty()) {
        // Auto-detect: use local IP
        ip = butil::my_ip_cstr();
    }
    if (port <= 0) {
        port = neokv::FLAGS_redis_port;
    }
    return ip + ":" + std::to_string(port);
#else
    return "127.0.0.1:16379";
#endif
}

// Generate a unique node ID (based on IP and port)
std::string generate_node_id() {
#ifdef NEOKV_REDIS_FULL
    std::string addr = get_redis_advertise_addr();
    // Simple hash-based node ID (40 hex chars like Redis)
    uint64_t hash = std::hash<std::string>{}(addr);
    char buf[41];
    snprintf(buf, sizeof(buf), "%016lx%016lx%08x", hash, hash ^ 0xDEADBEEFCAFEBABEULL, 
             static_cast<uint32_t>(hash >> 32));
    return std::string(buf, 40);
#else
    return "0000000000000000000000000000000000000001";
#endif
}

class ClusterCommandHandler final : public brpc::RedisCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        if (args.size() < 2) {
            output->SetError("ERR wrong number of arguments for 'cluster' command");
            return brpc::REDIS_CMD_HANDLED;
        }
        
        if (equals_ignore_case(args[1], "keyslot")) {
            return handle_keyslot(args, output);
        }
        if (equals_ignore_case(args[1], "info")) {
            return handle_info(args, output);
        }
        if (equals_ignore_case(args[1], "slots")) {
            return handle_slots(args, output);
        }
        if (equals_ignore_case(args[1], "nodes")) {
            return handle_nodes(args, output);
        }
        if (equals_ignore_case(args[1], "myid")) {
            output->SetString(generate_node_id());
            return brpc::REDIS_CMD_HANDLED;
        }
        
        output->SetError("ERR unknown subcommand or wrong number of arguments for 'cluster'");
        return brpc::REDIS_CMD_HANDLED;
    }

private:
    brpc::RedisCommandHandlerResult handle_keyslot(const std::vector<butil::StringPiece>& args,
                                                    brpc::RedisReply* output) {
        if (args.size() != 3) {
            output->SetError("ERR wrong number of arguments for 'cluster keyslot' command");
            return brpc::REDIS_CMD_HANDLED;
        }
        std::string key(args[2].data(), args[2].size());
        output->SetInteger(neokv::redis_slot(key));
        return brpc::REDIS_CMD_HANDLED;
    }

    brpc::RedisCommandHandlerResult handle_info(const std::vector<butil::StringPiece>& /*args*/,
                                                 brpc::RedisReply* output) {
#ifdef NEOKV_REDIS_FULL
        // Build CLUSTER INFO response with actual cluster state
        auto* store = neokv::Store::get_instance();
        int known_nodes = 1;
        int cluster_size = 1;
        
        if (store != nullptr) {
            // Count unique peers across all Redis regions
            std::set<std::string> peers;
            store->traverse_region_map([&](const neokv::SmartRegion& region) {
                const auto& info = region->region_info();
                for (const auto& peer : info.peers()) {
                    peers.insert(peer);
                }
            });
            if (!peers.empty()) {
                known_nodes = peers.size();
            }
        }
        
        std::ostringstream oss;
        oss << "cluster_state:ok\r\n"
            << "cluster_slots_assigned:16384\r\n"
            << "cluster_slots_ok:16384\r\n"
            << "cluster_slots_pfail:0\r\n"
            << "cluster_slots_fail:0\r\n"
            << "cluster_known_nodes:" << known_nodes << "\r\n"
            << "cluster_size:" << cluster_size << "\r\n"
            << "cluster_current_epoch:1\r\n"
            << "cluster_my_epoch:1\r\n";
        output->SetString(oss.str());
#else
        output->SetString(
            "cluster_state:ok\r\n"
            "cluster_slots_assigned:16384\r\n"
            "cluster_slots_ok:16384\r\n"
            "cluster_slots_pfail:0\r\n"
            "cluster_slots_fail:0\r\n"
            "cluster_known_nodes:1\r\n"
            "cluster_size:1\r\n"
        );
#endif
        return brpc::REDIS_CMD_HANDLED;
    }

    brpc::RedisCommandHandlerResult handle_slots(const std::vector<butil::StringPiece>& /*args*/,
                                                  brpc::RedisReply* output) {
#ifdef NEOKV_REDIS_FULL
        // CLUSTER SLOTS returns an array of slot ranges with node info
        // Format: [[start, end, [ip, port, node_id], [replica_ip, replica_port, replica_id]...], ...]
        
        auto* store = neokv::Store::get_instance();
        std::string advertise_addr = get_redis_advertise_addr();
        std::string node_id = generate_node_id();
        
        // Parse advertise address
        std::string advertise_ip = advertise_addr;
        int advertise_port = neokv::FLAGS_redis_port;
        auto colon_pos = advertise_addr.find(':');
        if (colon_pos != std::string::npos) {
            advertise_ip = advertise_addr.substr(0, colon_pos);
            advertise_port = std::stoi(advertise_addr.substr(colon_pos + 1));
        }
        
        // Collect slot ranges from regions
        struct SlotRange {
            uint16_t start;
            uint16_t end;
            std::string leader_ip;
            int leader_port;
        };
        std::vector<SlotRange> ranges;
        
        if (store != nullptr) {
            store->traverse_region_map([&](const neokv::SmartRegion& region) {
                const auto& info = region->region_info();
                // Parse start/end slots from region keys
                uint16_t start_slot = 0;
                uint16_t end_slot = 16383;
                
                const std::string& start_key = info.start_key();
                const std::string& end_key = info.end_key();
                
                if (!start_key.empty() && start_key.size() >= 2) {
                    uint16_t slot_be;
                    memcpy(&slot_be, start_key.data(), sizeof(slot_be));
                    start_slot = neokv::KeyEncoder::to_endian_u16(slot_be);
                }
                if (!end_key.empty() && end_key.size() >= 2) {
                    uint16_t slot_be;
                    memcpy(&slot_be, end_key.data(), sizeof(slot_be));
                    end_slot = neokv::KeyEncoder::to_endian_u16(slot_be);
                    if (end_slot > 0) end_slot--;  // end_key is exclusive
                }
                
                ranges.push_back({start_slot, end_slot, advertise_ip, advertise_port});
            });
        }
        
        // If no regions found, return full range for this node
        if (ranges.empty()) {
            ranges.push_back({0, 16383, advertise_ip, advertise_port});
        }
        
        // Build response array
        output->SetArray(static_cast<int>(ranges.size()));
        for (size_t i = 0; i < ranges.size(); ++i) {
            const auto& range = ranges[i];
            // Each slot range is: [start, end, [ip, port, node_id]]
            (*output)[i].SetArray(3);
            (*output)[i][0].SetInteger(range.start);
            (*output)[i][1].SetInteger(range.end);
            
            // Master node info: [ip, port, node_id]
            (*output)[i][2].SetArray(3);
            (*output)[i][2][0].SetString(range.leader_ip);
            (*output)[i][2][1].SetInteger(range.leader_port);
            (*output)[i][2][2].SetString(node_id);
        }
#else
        // Fallback: single node serving all slots
        output->SetArray(1);
        (*output)[0].SetArray(3);
        (*output)[0][0].SetInteger(0);
        (*output)[0][1].SetInteger(16383);
        (*output)[0][2].SetArray(3);
        (*output)[0][2][0].SetString("127.0.0.1");
        (*output)[0][2][1].SetInteger(16379);
        (*output)[0][2][2].SetString(generate_node_id());
#endif
        return brpc::REDIS_CMD_HANDLED;
    }

    brpc::RedisCommandHandlerResult handle_nodes(const std::vector<butil::StringPiece>& /*args*/,
                                                  brpc::RedisReply* output) {
#ifdef NEOKV_REDIS_FULL
        // CLUSTER NODES returns a bulk string with node info
        // Format: <node_id> <ip:port@cport> <flags> <master_id> <ping> <pong> <epoch> <link> <slot range>
        
        std::string advertise_addr = get_redis_advertise_addr();
        std::string node_id = generate_node_id();
        
        // Parse advertise address
        std::string advertise_ip = advertise_addr;
        int advertise_port = neokv::FLAGS_redis_port;
        auto colon_pos = advertise_addr.find(':');
        if (colon_pos != std::string::npos) {
            advertise_ip = advertise_addr.substr(0, colon_pos);
            advertise_port = std::stoi(advertise_addr.substr(colon_pos + 1));
        }
        
        // Cluster bus port is typically redis_port + 10000
        int cport = advertise_port + 10000;
        
        std::ostringstream oss;
        // node_id ip:port@cport flags master ping pong config-epoch link-state slots
        oss << node_id << " "
            << advertise_ip << ":" << advertise_port << "@" << cport << " "
            << "myself,master - "  // flags: this node is master
            << "0 "                // ping-sent
            << "0 "                // pong-recv
            << "1 "                // config-epoch
            << "connected "        // link-state
            << "0-16383\n";        // slot range
        
        output->SetString(oss.str());
#else
        std::string node_id = generate_node_id();
        std::ostringstream oss;
        oss << node_id << " 127.0.0.1:16379@26379 myself,master - 0 0 1 connected 0-16383\n";
        output->SetString(oss.str());
#endif
        return brpc::REDIS_CMD_HANDLED;
    }
};

// ============================================================================
// Phase 1 Command Handlers (GET/SET/DEL/MGET/MSET/EXPIRE/TTL)
// ============================================================================

#ifdef NEOKV_REDIS_FULL

// Standalone mode constants (used when no Redis table/regions exist)
// In standalone mode, we use fixed region_id and index_id for testing
// REDIS_INDEX_ID is defined in redis_common.h
static const int64_t STANDALONE_REGION_ID = 1;

// Base class for Redis data commands that need routing
class RedisDataCommandHandler : public brpc::RedisCommandHandler {
protected:
    static std::string store_addr_to_redis_addr(const std::string& store_addr) {
        // store_addr is usually "ip:store_port". Convert it to "ip:redis_port"
        // so Redis Cluster clients can follow MOVED/ASK redirects correctly.
        auto colon_pos = store_addr.find(':');
        if (colon_pos == std::string::npos) {
            return store_addr;
        }
        std::string ip = store_addr.substr(0, colon_pos);
        int redis_port = neokv::FLAGS_redis_advertise_port > 0
                ? neokv::FLAGS_redis_advertise_port
                : neokv::FLAGS_redis_port;
        return ip + ":" + std::to_string(redis_port);
    }

    static void set_moved_error(uint16_t slot, const std::string& leader_store_addr, brpc::RedisReply* output) {
        std::string leader_redis_addr = store_addr_to_redis_addr(leader_store_addr);
        std::string moved = "MOVED " + std::to_string(slot) + " " + leader_redis_addr;
        output->SetError(moved.c_str());
    }

    // Check routing and return error if needed
    // Returns true if routing succeeded, false if error was set
    // In standalone mode (no regions), returns true with synthetic route info
    bool check_route(const std::vector<butil::StringPiece>& args,
                     brpc::RedisReply* output,
                     neokv::RedisRouteResult& route) {
        auto* router = neokv::RedisRouter::get_instance();
        route = router->route(args);

        switch (route.status) {
            case neokv::RedisRouteResult::OK:
                return true;
            case neokv::RedisRouteResult::CROSSSLOT:
                output->SetError(route.error_msg.c_str());
                return false;
            case neokv::RedisRouteResult::CLUSTERDOWN:
                // Standalone mode: no regions exist, use fixed IDs for testing
                // This allows testing Redis without full cluster setup
                route.status = neokv::RedisRouteResult::OK;
                route.region_id = STANDALONE_REGION_ID;
                route.region = nullptr;  // No actual region in standalone mode
                DB_WARNING("Redis standalone mode: no region for slot %u, using region_id=%ld",
                           route.slot, route.region_id);
                return true;
            case neokv::RedisRouteResult::NOT_LEADER:
                output->SetError(route.error_msg.c_str());
                return false;
            case neokv::RedisRouteResult::INVALID_CMD:
                output->SetError(route.error_msg.c_str());
                return false;
            default:
                output->SetError("ERR internal error");
                return false;
        }
    }
    
    // Check if we can serve reads on this region
    // This follows the same flow as SQL's Region::query() for consistency:
    // - Leader: direct read (same as SQL)
    // - Follower/Learner with FLAGS_use_read_index: use ReadIndex for strong consistency
    // - Otherwise: return MOVED to leader
    // Returns true if read can proceed, false if error/redirect was set
    bool check_read_consistency(const neokv::RedisRouteResult& route,
                                brpc::RedisReply* output) {
        // Standalone mode: always allow reads
        if (route.region == nullptr) {
            return true;
        }
        
        // Leader: direct read (same as SQL's Region::query when is_leader() == true)
        if (route.region->is_leader()) {
            return true;
        }
        
        // --- Non-Leader: Follower or Learner ---
        // Following the same flow as SQL's Region::query() lines 1697-1730
        
        // Check Learner ready status (same as SQL: learner_ready_for_read() check)
        // This prevents reading from a learner that hasn't finished snapshot recovery
        if (route.region->is_learner() && !route.region->learner_ready_for_read()) {
            output->SetError("CLUSTERDOWN Learner not ready for read");
            return false;
        }
        
        // Use ReadIndex for strong consistency (same as SQL's use_read_idx flag)
        // Controlled by unified FLAGS_use_read_index
        if (neokv::FLAGS_use_read_index) {
            int64_t timeout_us = static_cast<int64_t>(neokv::FLAGS_follow_read_timeout_s) * 1000 * 1000LL;
            if (timeout_us <= 0) {
                timeout_us = 10 * 1000 * 1000LL;
            }
            neokv::pb::ErrCode err = route.region->follower_read_wait(0, timeout_us);
            if (err == neokv::pb::SUCCESS) {
                return true;
            }
            // ReadIndex failed - check specific error
            if (err == neokv::pb::LEARNER_NOT_READY) {
                output->SetError("CLUSTERDOWN Learner not ready for read");
                return false;
            }
            // Other errors (NOT_LEADER, timeout, etc.): redirect to leader
            set_moved_error(route.slot, butil::endpoint2str(route.region->get_leader()).c_str(), output);
            return false;
        }

        // Default behavior: Not leader and no ReadIndex - return MOVED to leader
        // This is the same as SQL when select_without_leader is false
        set_moved_error(route.slot, butil::endpoint2str(route.region->get_leader()).c_str(), output);
        return false;
    }
    
    // Get index_id for Redis operations
    // In standalone mode, returns fixed index_id
    int64_t get_index_id() {
        auto* router = neokv::RedisRouter::get_instance();
        int64_t index_id = router->get_redis_index_id();
        if (index_id <= 0) {
            // Use fixed Redis index_id (no SQL table needed)
            return neokv::REDIS_INDEX_ID;
        }
        return index_id;
    }

    // Read a key from RocksDB
    // Returns: 0 = found, 1 = not found, -1 = error
    int read_key(int64_t region_id, int64_t index_id, uint16_t slot,
                 const std::string& user_key,
                 std::string* value, int64_t* expire_at_ms) {
        auto* rocks = neokv::RocksWrapper::get_instance();
        if (rocks == nullptr) {
            return -1;
        }

        std::string rocks_key = neokv::RedisCodec::encode_key(
            region_id, index_id, slot, user_key, neokv::REDIS_STRING);

        rocksdb::ReadOptions read_options;
        std::string rocks_value;
        auto status = rocks->get(read_options, rocks->get_data_handle(),
                                 rocks_key, &rocks_value);

        if (status.IsNotFound()) {
            return 1;  // Key not found
        }
        if (!status.ok()) {
            DB_WARNING("RocksDB get failed: %s", status.ToString().c_str());
            return -1;
        }

        // Decode value
        int64_t expire_ms = 0;
        std::string payload;
        if (!neokv::RedisCodec::decode_value(rocks_value, &expire_ms, &payload)) {
            DB_WARNING("Failed to decode Redis value");
            return -1;
        }

        // Check expiration
        if (neokv::RedisCodec::is_expired(expire_ms, neokv::RedisCodec::current_time_ms())) {
            return 1;  // Expired, treat as not found
        }

        if (value) *value = std::move(payload);
        if (expire_at_ms) *expire_at_ms = expire_ms;
        return 0;
    }
    
    // Write through Raft (for strong consistency)
    // Returns 0 on success, -1 on error
    // If route.region is nullptr (standalone mode), falls back to direct write
    int write_through_raft(const neokv::RedisRouteResult& route,
                           neokv::pb::RedisCmd cmd,
                           const std::vector<std::pair<std::string, std::string>>& kvs,
                           int64_t expire_ms,
                           brpc::RedisReply* output,
                           int64_t* affected_count = nullptr) {
        // Build RedisWriteRequest
        neokv::pb::RedisWriteRequest redis_req;
        redis_req.set_cmd(cmd);
        redis_req.set_slot(route.slot);
        
        for (const auto& kv : kvs) {
            auto* kv_pb = redis_req.add_kvs();
            kv_pb->set_key(kv.first);
            if (!kv.second.empty()) {
                kv_pb->set_value(kv.second);
            }
            if (expire_ms != neokv::REDIS_NO_EXPIRE) {
                kv_pb->set_expire_ms(expire_ms);
            }
        }
        
        // If we have a region, use Raft path
        if (route.region != nullptr) {
            neokv::pb::StoreRes response;
            int ret = route.region->exec_redis_write(redis_req, &response);
            
            if (ret != 0) {
                // Check for NOT_LEADER and return MOVED
                if (response.errcode() == neokv::pb::NOT_LEADER) {
                    set_moved_error(route.slot, response.leader(), output);
                } else {
                    output->SetError(response.errmsg().c_str());
                }
                return -1;
            }
            
            if (affected_count) {
                *affected_count = response.affected_rows();
            }
            return 0;
        }
        
        // Standalone mode: direct write to RocksDB (for testing only)
        auto* rocks = neokv::RocksWrapper::get_instance();
        if (rocks == nullptr) {
            output->SetError("ERR internal error");
            return -1;
        }
        
        rocksdb::WriteBatch batch;
        int64_t affected = 0;
        int64_t index_id = get_index_id();
        
        for (const auto& kv : kvs) {
            std::string rocks_key = neokv::RedisCodec::encode_key(
                route.region_id, index_id, route.slot, kv.first, neokv::REDIS_STRING);
            
            switch (cmd) {
                case neokv::pb::REDIS_SET:
                case neokv::pb::REDIS_MSET: {
                    std::string rocks_value = neokv::RedisCodec::encode_value(expire_ms, kv.second);
                    batch.Put(rocks->get_data_handle(), rocks_key, rocks_value);
                    ++affected;
                    break;
                }
                case neokv::pb::REDIS_DEL: {
                    rocksdb::ReadOptions read_options;
                    std::string existing;
                    auto status = rocks->get(read_options, rocks->get_data_handle(), rocks_key, &existing);
                    if (status.ok()) {
                        int64_t exp = 0;
                        std::string payload;
                        if (neokv::RedisCodec::decode_value(existing, &exp, &payload) &&
                            !neokv::RedisCodec::is_expired(exp, neokv::RedisCodec::current_time_ms())) {
                            batch.Delete(rocks->get_data_handle(), rocks_key);
                            ++affected;
                        }
                    }
                    break;
                }
                case neokv::pb::REDIS_EXPIRE:
                case neokv::pb::REDIS_EXPIREAT: {
                    rocksdb::ReadOptions read_options;
                    std::string existing;
                    auto status = rocks->get(read_options, rocks->get_data_handle(), rocks_key, &existing);
                    if (status.ok()) {
                        int64_t old_exp = 0;
                        std::string payload;
                        if (neokv::RedisCodec::decode_value(existing, &old_exp, &payload) &&
                            !neokv::RedisCodec::is_expired(old_exp, neokv::RedisCodec::current_time_ms())) {
                            std::string new_value = neokv::RedisCodec::encode_value(expire_ms, payload);
                            batch.Put(rocks->get_data_handle(), rocks_key, new_value);
                            ++affected;
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }
        
        rocksdb::WriteOptions write_options;
        auto status = rocks->write(write_options, &batch);
        
        if (!status.ok()) {
            output->SetError("ERR write failed");
            return -1;
        }
        
        if (affected_count) {
            *affected_count = affected;
        }
        return 0;
    }
};

class GetCommandHandler final : public RedisDataCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        if (args.size() != 2) {
            output->SetError("ERR wrong number of arguments for 'get' command");
            return brpc::REDIS_CMD_HANDLED;
        }

        neokv::RedisRouteResult route;
        if (!check_route(args, output, route)) {
            return brpc::REDIS_CMD_HANDLED;
        }
        
        // Check read consistency (leader lease check)
        if (!check_read_consistency(route, output)) {
            return brpc::REDIS_CMD_HANDLED;
        }

        std::string user_key(args[1].data(), args[1].size());
        std::string value;

        int ret = read_key(route.region_id, get_index_id(),
                           route.slot, user_key, &value, nullptr);

        if (ret == 0) {
            output->SetString(value);
        } else if (ret == 1) {
            output->SetNullString();
        } else {
            output->SetError("ERR internal error");
        }

        return brpc::REDIS_CMD_HANDLED;
    }
};

class MgetCommandHandler final : public RedisDataCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        if (args.size() < 2) {
            output->SetError("ERR wrong number of arguments for 'mget' command");
            return brpc::REDIS_CMD_HANDLED;
        }

        neokv::RedisRouteResult route;
        if (!check_route(args, output, route)) {
            return brpc::REDIS_CMD_HANDLED;
        }
        
        // Check read consistency (leader lease check)
        if (!check_read_consistency(route, output)) {
            return brpc::REDIS_CMD_HANDLED;
        }

        size_t num_keys = args.size() - 1;
        output->SetArray(static_cast<int>(num_keys));

        for (size_t i = 0; i < num_keys; ++i) {
            std::string user_key(args[i + 1].data(), args[i + 1].size());
            std::string value;

            int ret = read_key(route.region_id, get_index_id(),
                               route.slot, user_key, &value, nullptr);

            if (ret == 0) {
                (*output)[i].SetString(value);
            } else {
                (*output)[i].SetNullString();
            }
        }

        return brpc::REDIS_CMD_HANDLED;
    }
};

class TtlCommandHandler final : public RedisDataCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        if (args.size() != 2) {
            output->SetError("ERR wrong number of arguments for 'ttl' command");
            return brpc::REDIS_CMD_HANDLED;
        }

        neokv::RedisRouteResult route;
        if (!check_route(args, output, route)) {
            return brpc::REDIS_CMD_HANDLED;
        }
        
        // Check read consistency (leader lease check)
        if (!check_read_consistency(route, output)) {
            return brpc::REDIS_CMD_HANDLED;
        }

        std::string user_key(args[1].data(), args[1].size());
        int64_t expire_at_ms = 0;

        int ret = read_key(route.region_id, get_index_id(),
                           route.slot, user_key, nullptr, &expire_at_ms);

        if (ret == 1) {
            // Key not found
            output->SetInteger(-2);
        } else if (ret == 0) {
            int64_t ttl = neokv::RedisCodec::compute_ttl_seconds(expire_at_ms);
            output->SetInteger(ttl);
        } else {
            output->SetError("ERR internal error");
        }

        return brpc::REDIS_CMD_HANDLED;
    }
};

// Write commands - these go through Raft for strong consistency

class SetCommandHandler final : public RedisDataCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        // SET key value [EX seconds] [PX milliseconds] [NX|XX]
        if (args.size() < 3) {
            output->SetError("ERR wrong number of arguments for 'set' command");
            return brpc::REDIS_CMD_HANDLED;
        }

        neokv::RedisRouteResult route;
        if (!check_route(args, output, route)) {
            return brpc::REDIS_CMD_HANDLED;
        }

        std::string user_key(args[1].data(), args[1].size());
        std::string user_value(args[2].data(), args[2].size());
        int64_t expire_at_ms = neokv::REDIS_NO_EXPIRE;

        // Parse options (simplified)
        for (size_t i = 3; i < args.size(); ++i) {
            if (equals_ignore_case(args[i], "ex") && i + 1 < args.size()) {
                try {
                    int64_t seconds = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
                    expire_at_ms = neokv::RedisCodec::current_time_ms() + seconds * 1000;
                } catch (...) {
                    output->SetError("ERR value is not an integer or out of range");
                    return brpc::REDIS_CMD_HANDLED;
                }
                ++i;
            } else if (equals_ignore_case(args[i], "px") && i + 1 < args.size()) {
                try {
                    int64_t ms = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
                    expire_at_ms = neokv::RedisCodec::current_time_ms() + ms;
                } catch (...) {
                    output->SetError("ERR value is not an integer or out of range");
                    return brpc::REDIS_CMD_HANDLED;
                }
                ++i;
            }
            // TODO: Handle NX/XX options
        }

        std::vector<std::pair<std::string, std::string>> kvs;
        kvs.emplace_back(user_key, user_value);
        
        int ret = write_through_raft(route, neokv::pb::REDIS_SET, kvs, expire_at_ms, output);
        if (ret == 0) {
            output->SetStatus("OK");
        }
        // Error already set by write_through_raft

        return brpc::REDIS_CMD_HANDLED;
    }
};

class DelCommandHandler final : public RedisDataCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        if (args.size() < 2) {
            output->SetError("ERR wrong number of arguments for 'del' command");
            return brpc::REDIS_CMD_HANDLED;
        }

        neokv::RedisRouteResult route;
        if (!check_route(args, output, route)) {
            return brpc::REDIS_CMD_HANDLED;
        }

        std::vector<std::pair<std::string, std::string>> kvs;
        for (size_t i = 1; i < args.size(); ++i) {
            kvs.emplace_back(std::string(args[i].data(), args[i].size()), "");
        }
        
        int64_t deleted = 0;
        int ret = write_through_raft(route, neokv::pb::REDIS_DEL, kvs, 
                                     neokv::REDIS_NO_EXPIRE, output, &deleted);
        if (ret == 0) {
            output->SetInteger(deleted);
        }
        // Error already set by write_through_raft

        return brpc::REDIS_CMD_HANDLED;
    }
};

class MsetCommandHandler final : public RedisDataCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        // MSET key value [key value ...]
        if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
            output->SetError("ERR wrong number of arguments for 'mset' command");
            return brpc::REDIS_CMD_HANDLED;
        }

        neokv::RedisRouteResult route;
        if (!check_route(args, output, route)) {
            return brpc::REDIS_CMD_HANDLED;
        }

        std::vector<std::pair<std::string, std::string>> kvs;
        for (size_t i = 1; i < args.size(); i += 2) {
            kvs.emplace_back(
                std::string(args[i].data(), args[i].size()),
                std::string(args[i + 1].data(), args[i + 1].size()));
        }
        
        int ret = write_through_raft(route, neokv::pb::REDIS_MSET, kvs,
                                     neokv::REDIS_NO_EXPIRE, output);
        if (ret == 0) {
            output->SetStatus("OK");
        }
        // Error already set by write_through_raft

        return brpc::REDIS_CMD_HANDLED;
    }
};

class ExpireCommandHandler final : public RedisDataCommandHandler {
public:
    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output,
                                        bool /*flush_batched*/) override {
        if (args.size() != 3) {
            output->SetError("ERR wrong number of arguments for 'expire' command");
            return brpc::REDIS_CMD_HANDLED;
        }

        neokv::RedisRouteResult route;
        if (!check_route(args, output, route)) {
            return brpc::REDIS_CMD_HANDLED;
        }

        std::string user_key(args[1].data(), args[1].size());
        int64_t seconds = 0;
        try {
            seconds = std::stoll(std::string(args[2].data(), args[2].size()));
        } catch (...) {
            output->SetError("ERR value is not an integer or out of range");
            return brpc::REDIS_CMD_HANDLED;
        }

        // Check if key exists first (for proper return value)
        std::string value;
        int ret = read_key(route.region_id, get_index_id(), route.slot, user_key, &value, nullptr);
        if (ret != 0) {
            output->SetInteger(0);  // Key doesn't exist or error
            return brpc::REDIS_CMD_HANDLED;
        }

        // Build EXPIRE request through Raft
        int64_t new_expire = neokv::RedisCodec::current_time_ms() + seconds * 1000;
        std::vector<std::pair<std::string, std::string>> kvs;
        kvs.emplace_back(user_key, "");
        
        int64_t affected = 0;
        ret = write_through_raft(route, neokv::pb::REDIS_EXPIRE, kvs, new_expire, output, &affected);
        if (ret == 0) {
            output->SetInteger(affected > 0 ? 1 : 0);
        }
        // Error already set by write_through_raft

        return brpc::REDIS_CMD_HANDLED;
    }
};

#endif // NEOKV_REDIS_FULL

// ============================================================================
// Command Registration
// ============================================================================

std::vector<std::unique_ptr<brpc::RedisCommandHandler>>& command_holders() {
    static std::vector<std::unique_ptr<brpc::RedisCommandHandler>> handlers;
    return handlers;
}

bool add_handler(brpc::RedisService* service,
                 std::unique_ptr<brpc::RedisCommandHandler> handler,
                 std::initializer_list<const char*> names) {
    if (service == nullptr) {
        return false;
    }
    brpc::RedisCommandHandler* raw_handler = handler.get();
    for (const auto* name : names) {
        if (!service->AddCommandHandler(name, raw_handler)) {
            return false;
        }
    }
    command_holders().push_back(std::move(handler));
    return true;
}

} // namespace

namespace neokv {

uint16_t redis_crc16(const std::string& input) {
    uint16_t crc = 0;
    for (const unsigned char ch : input) {
        crc ^= static_cast<uint16_t>(ch) << 8;
        for (int i = 0; i < 8; ++i) {
            if (crc & 0x8000) {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}

uint16_t redis_slot(const std::string& key) {
    const std::string tag = extract_hash_tag(key);
    return redis_crc16(tag) & 0x3FFF;
}

bool register_basic_redis_commands(brpc::RedisService* service) {
    if (service == nullptr) {
        return false;
    }

    // Phase 0: Basic commands (always available)
    if (!add_handler(service, std::make_unique<PingCommandHandler>(), {"ping"})) {
        DB_FATAL("failed to register PING command");
        return false;
    }
    if (!add_handler(service, std::make_unique<EchoCommandHandler>(), {"echo"})) {
        DB_FATAL("failed to register ECHO command");
        return false;
    }
    if (!add_handler(service, std::make_unique<ClusterCommandHandler>(), {"cluster"})) {
        DB_FATAL("failed to register CLUSTER command");
        return false;
    }

#ifdef NEOKV_REDIS_FULL
    // Phase 1: Data commands (requires full NEOKV integration)
    if (!add_handler(service, std::make_unique<GetCommandHandler>(), {"get"})) {
        DB_FATAL("failed to register GET command");
        return false;
    }
    if (!add_handler(service, std::make_unique<SetCommandHandler>(), {"set"})) {
        DB_FATAL("failed to register SET command");
        return false;
    }
    if (!add_handler(service, std::make_unique<DelCommandHandler>(), {"del"})) {
        DB_FATAL("failed to register DEL command");
        return false;
    }
    if (!add_handler(service, std::make_unique<MgetCommandHandler>(), {"mget"})) {
        DB_FATAL("failed to register MGET command");
        return false;
    }
    if (!add_handler(service, std::make_unique<MsetCommandHandler>(), {"mset"})) {
        DB_FATAL("failed to register MSET command");
        return false;
    }
    if (!add_handler(service, std::make_unique<ExpireCommandHandler>(), {"expire"})) {
        DB_FATAL("failed to register EXPIRE command");
        return false;
    }
    if (!add_handler(service, std::make_unique<TtlCommandHandler>(), {"ttl"})) {
        DB_FATAL("failed to register TTL command");
        return false;
    }
    DB_WARNING("Redis Phase 1 commands registered: GET/SET/DEL/MGET/MSET/EXPIRE/TTL");
#endif

    return true;
}

} // namespace neokv
