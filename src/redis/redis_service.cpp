// Enable full NEOKV integration for Redis commands
#define NEOKV_REDIS_FULL

#include "redis_service.h"
#include "redis_codec.h"
#include "redis_metadata.h"
#include "redis_router.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
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
#include <random>
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
		if (std::tolower(static_cast<unsigned char>(lhs[i])) != std::tolower(static_cast<unsigned char>(rhs[i]))) {
			return false;
		}
	}
	return true;
}

bool split_ip_port(const std::string& addr, std::string* ip, int* port) {
	const auto colon_pos = addr.find(':');
	if (colon_pos == std::string::npos) {
		return false;
	}
	try {
		*ip = addr.substr(0, colon_pos);
		*port = std::stoi(addr.substr(colon_pos + 1));
	} catch (...) {
		return false;
	}
	return !ip->empty() && *port > 0;
}

// ============================================================================
// Phase 0 Command Handlers (PING/ECHO/CLUSTER KEYSLOT)
// ============================================================================

class PingCommandHandler final : public brpc::RedisCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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

// Convert a Store RPC address (ip:store_port) to a Redis client address (ip:redis_port).
// LIMITATION: Assumes all nodes use the same redis_port / redis_advertise_port.
// If different nodes configure different redis_port values, MOVED redirects will
// point to the wrong port. To fix this properly, RegionInfo.peers should carry
// per-node Redis port information (see design doc §4.3).
std::string store_addr_to_redis_addr(const std::string& store_addr) {
#ifdef NEOKV_REDIS_FULL
	std::string ip;
	int port = 0;
	if (!split_ip_port(store_addr, &ip, &port)) {
		return get_redis_advertise_addr();
	}
	int redis_port =
	    neokv::FLAGS_redis_advertise_port > 0 ? neokv::FLAGS_redis_advertise_port : neokv::FLAGS_redis_port;
	return ip + ":" + std::to_string(redis_port);
#else
	(void)store_addr;
	return "127.0.0.1:16379";
#endif
}

std::string generate_node_id_for_addr(const std::string& addr) {
	uint64_t hash = std::hash<std::string> {}(addr);
	char buf[41];
	snprintf(buf, sizeof(buf), "%016lx%016lx%08x", static_cast<unsigned long>(hash),
	         static_cast<unsigned long>(hash ^ 0xDEADBEEFCAFEBABEULL), static_cast<uint32_t>(hash >> 32));
	return std::string(buf, 40);
}

// Generate a unique node ID (based on IP and port)
std::string generate_node_id() {
	return generate_node_id_for_addr(get_redis_advertise_addr());
}

#ifdef NEOKV_REDIS_FULL
struct SlotRangeView {
	uint16_t start = 0;
	uint16_t end = 16383; // inclusive
	std::string leader_store_addr;
	std::vector<std::string> peers;
};

void decode_slot_range(const neokv::pb::RegionInfo& info, uint16_t* start, uint16_t* end) {
	*start = 0;
	*end = 16383;
	if (!info.start_key().empty() && info.start_key().size() >= sizeof(uint16_t)) {
		uint16_t slot_be = 0;
		memcpy(&slot_be, info.start_key().data(), sizeof(slot_be));
		*start = neokv::KeyEncoder::to_endian_u16(slot_be);
	}
	if (!info.end_key().empty() && info.end_key().size() >= sizeof(uint16_t)) {
		uint16_t slot_be = 0;
		memcpy(&slot_be, info.end_key().data(), sizeof(slot_be));
		uint16_t end_exclusive = neokv::KeyEncoder::to_endian_u16(slot_be);
		*end = end_exclusive == 0 ? 0 : static_cast<uint16_t>(end_exclusive - 1);
	}
}

std::vector<SlotRangeView> collect_slot_ranges() {
	std::vector<SlotRangeView> ranges;
	auto* store = neokv::Store::get_instance();
	if (store == nullptr) {
		return ranges;
	}
	store->traverse_region_map([&](const neokv::SmartRegion& region) {
		if (region == nullptr || region->get_table_id() <= 0) {
			return;
		}
		SlotRangeView r;
		const auto& info = region->region_info();
		decode_slot_range(info, &r.start, &r.end);
		r.leader_store_addr = info.leader();
		if (r.leader_store_addr.empty() || r.leader_store_addr == "0.0.0.0:0") {
			r.leader_store_addr = butil::endpoint2str(region->get_leader()).c_str();
		}
		for (const auto& peer : info.peers()) {
			if (!peer.empty()) {
				r.peers.push_back(peer);
			}
		}
		if (r.leader_store_addr.empty() && !r.peers.empty()) {
			r.leader_store_addr = r.peers.front();
		}
		ranges.push_back(std::move(r));
	});
	std::sort(ranges.begin(), ranges.end(), [](const SlotRangeView& a, const SlotRangeView& b) {
		if (a.start != b.start) {
			return a.start < b.start;
		}
		return a.end < b.end;
	});
	return ranges;
}
#endif

class ClusterCommandHandler final : public brpc::RedisCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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
		std::set<std::string> known_nodes;
		std::set<std::string> leader_nodes;
		std::array<bool, neokv::REDIS_SLOT_COUNT> assigned_slots {};
		assigned_slots.fill(false);

		for (const auto& range : collect_slot_ranges()) {
			for (const auto& peer : range.peers) {
				known_nodes.insert(peer);
			}
			if (!range.leader_store_addr.empty()) {
				leader_nodes.insert(range.leader_store_addr);
			}
			for (uint32_t slot = range.start; slot <= range.end && slot < neokv::REDIS_SLOT_COUNT; ++slot) {
				assigned_slots[slot] = true;
			}
		}

		int slots_assigned = 0;
		for (bool assigned : assigned_slots) {
			slots_assigned += assigned ? 1 : 0;
		}
		const bool cluster_ok = slots_assigned == neokv::REDIS_SLOT_COUNT;
		const int known_nodes_num = std::max<int>(1, static_cast<int>(known_nodes.size()));
		const int cluster_size = std::max<int>(1, static_cast<int>(leader_nodes.size()));

		std::ostringstream oss;
		oss << "cluster_state:" << (cluster_ok ? "ok" : "fail") << "\r\n"
		    << "cluster_slots_assigned:" << slots_assigned << "\r\n"
		    << "cluster_slots_ok:" << slots_assigned << "\r\n"
		    << "cluster_slots_pfail:0\r\n"
		    << "cluster_slots_fail:0\r\n"
		    << "cluster_known_nodes:" << known_nodes_num << "\r\n"
		    << "cluster_size:" << cluster_size << "\r\n"
		    << "cluster_current_epoch:1\r\n"
		    << "cluster_my_epoch:1\r\n"
		    << "cluster_stats_messages_sent:0\r\n"
		    << "cluster_stats_messages_received:0\r\n"
		    << "total_cluster_links_buffer_limit_exceeded:0\r\n";
		output->SetString(oss.str());
#else
		output->SetString("cluster_state:ok\r\n"
		                  "cluster_slots_assigned:16384\r\n"
		                  "cluster_slots_ok:16384\r\n"
		                  "cluster_slots_pfail:0\r\n"
		                  "cluster_slots_fail:0\r\n"
		                  "cluster_known_nodes:1\r\n"
		                  "cluster_size:1\r\n");
#endif
		return brpc::REDIS_CMD_HANDLED;
	}

	brpc::RedisCommandHandlerResult handle_slots(const std::vector<butil::StringPiece>& /*args*/,
	                                             brpc::RedisReply* output) {
#ifdef NEOKV_REDIS_FULL
		const auto ranges = collect_slot_ranges();
		if (ranges.empty()) {
			output->SetArray(1);
			(*output)[0].SetArray(3);
			(*output)[0][0].SetInteger(0);
			(*output)[0][1].SetInteger(16383);
			(*output)[0][2].SetArray(3);
			std::string ip;
			int port = 0;
			const std::string advertise_addr = get_redis_advertise_addr();
			if (!split_ip_port(advertise_addr, &ip, &port)) {
				ip = "127.0.0.1";
				port = neokv::FLAGS_redis_port > 0 ? neokv::FLAGS_redis_port : 16379;
			}
			(*output)[0][2][0].SetString(ip);
			(*output)[0][2][1].SetInteger(port);
			(*output)[0][2][2].SetString(generate_node_id());
			return brpc::REDIS_CMD_HANDLED;
		}

		output->SetArray(static_cast<int>(ranges.size()));
		for (size_t i = 0; i < ranges.size(); ++i) {
			const auto& range = ranges[i];
			std::string leader_redis_addr = store_addr_to_redis_addr(range.leader_store_addr);
			std::string leader_ip;
			int leader_port = 0;
			if (!split_ip_port(leader_redis_addr, &leader_ip, &leader_port)) {
				leader_ip = "127.0.0.1";
				leader_port = neokv::FLAGS_redis_port > 0 ? neokv::FLAGS_redis_port : 16379;
			}

			std::vector<std::string> replica_addrs;
			std::set<std::string> dedup;
			for (const auto& peer : range.peers) {
				if (peer.empty() || peer == range.leader_store_addr) {
					continue;
				}
				const std::string redis_addr = store_addr_to_redis_addr(peer);
				if (!dedup.insert(redis_addr).second) {
					continue;
				}
				replica_addrs.push_back(redis_addr);
			}

			const int entry_size = 3 + static_cast<int>(replica_addrs.size());
			(*output)[i].SetArray(entry_size);
			(*output)[i][0].SetInteger(range.start);
			(*output)[i][1].SetInteger(range.end);
			(*output)[i][2].SetArray(3);
			(*output)[i][2][0].SetString(leader_ip);
			(*output)[i][2][1].SetInteger(leader_port);
			(*output)[i][2][2].SetString(generate_node_id_for_addr(leader_redis_addr));
			for (size_t j = 0; j < replica_addrs.size(); ++j) {
				std::string replica_ip;
				int replica_port = 0;
				if (!split_ip_port(replica_addrs[j], &replica_ip, &replica_port)) {
					continue;
				}
				(*output)[i][3 + static_cast<int>(j)].SetArray(3);
				(*output)[i][3 + static_cast<int>(j)][0].SetString(replica_ip);
				(*output)[i][3 + static_cast<int>(j)][1].SetInteger(replica_port);
				(*output)[i][3 + static_cast<int>(j)][2].SetString(generate_node_id_for_addr(replica_addrs[j]));
			}
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
		const auto ranges = collect_slot_ranges();
		const std::string self_redis_addr = get_redis_advertise_addr();

		std::unordered_map<std::string, std::vector<std::pair<uint16_t, uint16_t>>> leader_slots;
		std::set<std::string> all_nodes;
		for (const auto& range : ranges) {
			if (!range.leader_store_addr.empty()) {
				const std::string leader_redis_addr = store_addr_to_redis_addr(range.leader_store_addr);
				leader_slots[leader_redis_addr].push_back({range.start, range.end});
				all_nodes.insert(leader_redis_addr);
			}
			for (const auto& peer : range.peers) {
				if (peer.empty()) {
					continue;
				}
				all_nodes.insert(store_addr_to_redis_addr(peer));
			}
		}
		if (all_nodes.empty()) {
			all_nodes.insert(self_redis_addr);
			leader_slots[self_redis_addr].push_back({0, 16383});
		}

		std::ostringstream oss;
		for (const auto& node_addr : all_nodes) {
			std::string ip;
			int port = 0;
			if (!split_ip_port(node_addr, &ip, &port)) {
				continue;
			}
			const bool is_master = leader_slots.find(node_addr) != leader_slots.end();
			const bool is_self = node_addr == self_redis_addr;
			const std::string flags =
			    is_self ? (is_master ? "myself,master" : "myself,slave") : (is_master ? "master" : "slave");
			const std::string node_id = generate_node_id_for_addr(node_addr);
			const int cport = port + 10000;
			oss << node_id << " " << ip << ":" << port << "@" << cport << " " << flags << " " << "- "
			    << "0 0 1 connected";
			if (is_master) {
				for (const auto& slot_range : leader_slots[node_addr]) {
					oss << " " << slot_range.first << "-" << slot_range.second;
				}
			}
			oss << "\n";
		}
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

// Base class for Redis data commands that need routing
class RedisDataCommandHandler : public brpc::RedisCommandHandler {
public:
	static void set_moved_error(uint16_t slot, const std::string& leader_store_addr, brpc::RedisReply* output) {
		std::string leader_redis_addr = store_addr_to_redis_addr(leader_store_addr);
		std::string moved = "MOVED " + std::to_string(slot) + " " + leader_redis_addr;
		output->SetError(moved.c_str());
	}

protected:
	// Check routing and return error if needed
	// Returns true if routing succeeded, false if error was set
	bool check_route(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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
			output->SetError(route.error_msg.c_str());
			return false;
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
	bool check_read_consistency(const neokv::RedisRouteResult& route, brpc::RedisReply* output) {
		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return false;
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

	static int64_t get_index_id(const neokv::RedisRouteResult& route) {
		if (route.index_id > 0) {
			return route.index_id;
		}
		if (route.region != nullptr) {
			return route.region->get_table_id();
		}
		return 0;
	}

	// Read a key from RocksDB (metadata CF first, fallback to data CF)
	// Returns: 0 = found, 1 = not found, -1 = error
	int read_key(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key, std::string* value,
	             int64_t* expire_at_ms) {
		if (index_id <= 0) {
			return -1;
		}
		auto* rocks = neokv::RocksWrapper::get_instance();
		if (rocks == nullptr) {
			return -1;
		}

		rocksdb::ReadOptions read_options;

		// Try metadata CF first (new encoding)
		std::string meta_key = neokv::RedisMetadataKey::encode(region_id, index_id, slot, user_key);
		std::string raw_value;
		auto status = rocks->get(read_options, rocks->get_redis_metadata_handle(), meta_key, &raw_value);

		if (status.ok()) {
			neokv::RedisMetadata meta(neokv::kRedisNone, false);
			const char* ptr = raw_value.data();
			size_t remaining = raw_value.size();
			if (!meta.decode(&ptr, &remaining)) {
				DB_WARNING("Failed to decode Redis metadata");
				return -1;
			}
			if (meta.type() != neokv::kRedisString) {
				// Wrong type — treat as not found for String operations
				return 1;
			}
			if (meta.is_expired()) {
				return 1;
			}
			if (value) {
				value->assign(ptr, remaining);
			}
			if (expire_at_ms) {
				*expire_at_ms = (meta.expire > 0) ? static_cast<int64_t>(meta.expire) : 0;
			}
			return 0;
		}

		if (!status.IsNotFound()) {
			DB_WARNING("RocksDB metadata get failed: %s", status.ToString().c_str());
			return -1;
		}

		// Fallback: try old data CF encoding
		std::string rocks_key = neokv::RedisCodec::encode_key(region_id, index_id, slot, user_key, neokv::REDIS_STRING);
		std::string rocks_value;
		status = rocks->get(read_options, rocks->get_data_handle(), rocks_key, &rocks_value);

		if (status.IsNotFound()) {
			return 1; // Key not found
		}
		if (!status.ok()) {
			DB_WARNING("RocksDB get failed: %s", status.ToString().c_str());
			return -1;
		}

		// Decode old-format value
		int64_t expire_ms = 0;
		std::string payload;
		if (!neokv::RedisCodec::decode_value(rocks_value, &expire_ms, &payload)) {
			DB_WARNING("Failed to decode Redis value");
			return -1;
		}

		// Check expiration
		if (neokv::RedisCodec::is_expired(expire_ms, neokv::RedisCodec::current_time_ms())) {
			return 1; // Expired, treat as not found
		}

		if (value)
			*value = std::move(payload);
		if (expire_at_ms)
			*expire_at_ms = expire_ms;
		return 0;
	}

	// Write through Raft (for strong consistency)
	// Returns 0 on success, -1 on error
	int write_through_raft(const neokv::RedisRouteResult& route, neokv::pb::RedisCmd cmd,
	                       const std::vector<std::pair<std::string, std::string>>& kvs, int64_t expire_ms,
	                       brpc::RedisReply* output, int64_t* affected_count = nullptr,
	                       neokv::pb::RedisSetCondition set_condition = neokv::pb::REDIS_SET_CONDITION_NONE) {
		// Build RedisWriteRequest
		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(cmd);
		redis_req.set_slot(route.slot);
		if (cmd == neokv::pb::REDIS_SET) {
			redis_req.set_set_condition(set_condition);
		}

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

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return -1;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);

		if (ret != 0) {
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
};

class GetCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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

		int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, &value, nullptr);

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
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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

			int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, &value, nullptr);

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
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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

		int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, nullptr, &expire_at_ms);

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
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SET key value [EX seconds] [PX milliseconds] [NX|XX]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'set' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		std::string user_value(args[2].data(), args[2].size());
		int64_t expire_at_ms = neokv::REDIS_NO_EXPIRE;

		bool has_ex = false;
		bool has_px = false;
		bool nx = false;
		bool xx = false;
		for (size_t i = 3; i < args.size(); ++i) {
			if (equals_ignore_case(args[i], "ex") || equals_ignore_case(args[i], "px")) {
				const bool is_ex = equals_ignore_case(args[i], "ex");
				if (i + 1 >= args.size()) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				if ((is_ex && has_ex) || (!is_ex && has_px) || (is_ex && has_px) || (!is_ex && has_ex)) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				int64_t ttl_part = 0;
				try {
					ttl_part = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (ttl_part <= 0) {
					output->SetError("ERR invalid expire time in 'set' command");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (is_ex) {
					has_ex = true;
					expire_at_ms = neokv::RedisCodec::current_time_ms() + ttl_part * 1000;
				} else {
					has_px = true;
					expire_at_ms = neokv::RedisCodec::current_time_ms() + ttl_part;
				}
				++i;
				continue;
			}
			if (equals_ignore_case(args[i], "nx")) {
				if (nx || xx) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				nx = true;
				continue;
			}
			if (equals_ignore_case(args[i], "xx")) {
				if (xx || nx) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				xx = true;
				continue;
			}
			output->SetError("ERR syntax error");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, user_value);

		neokv::pb::RedisSetCondition set_condition = neokv::pb::REDIS_SET_CONDITION_NONE;
		if (nx) {
			set_condition = neokv::pb::REDIS_SET_CONDITION_NX;
		} else if (xx) {
			set_condition = neokv::pb::REDIS_SET_CONDITION_XX;
		}

		int64_t affected = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_SET, kvs, expire_at_ms, output, &affected, set_condition);
		if (ret == 0) {
			if (set_condition != neokv::pb::REDIS_SET_CONDITION_NONE && affected == 0) {
				output->SetNullString();
			} else {
				output->SetStatus("OK");
			}
		}
		// Error already set by write_through_raft

		return brpc::REDIS_CMD_HANDLED;
	}
};

class DelCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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
		int ret = write_through_raft(route, neokv::pb::REDIS_DEL, kvs, neokv::REDIS_NO_EXPIRE, output, &deleted);
		if (ret == 0) {
			output->SetInteger(deleted);
		}
		// Error already set by write_through_raft

		return brpc::REDIS_CMD_HANDLED;
	}
};

class MsetCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
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
			kvs.emplace_back(std::string(args[i].data(), args[i].size()),
			                 std::string(args[i + 1].data(), args[i + 1].size()));
		}

		int ret = write_through_raft(route, neokv::pb::REDIS_MSET, kvs, neokv::REDIS_NO_EXPIRE, output);
		if (ret == 0) {
			output->SetStatus("OK");
		}
		// Error already set by write_through_raft

		return brpc::REDIS_CMD_HANDLED;
	}
};

class ExpireCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'expire' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		int64_t seconds = 0;
		try {
			seconds = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		int64_t new_expire = neokv::RedisCodec::current_time_ms() + seconds * 1000;
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, "");

		int64_t affected = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_EXPIRE, kvs, new_expire, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected > 0 ? 1 : 0);
		}
		// Error already set by write_through_raft

		return brpc::REDIS_CMD_HANDLED;
	}
};

// FlushDB: delete all keys in all regions.
// This iterates all regions and sends a REDIS_FLUSHDB write through Raft for each.
class FlushdbCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 1) {
			output->SetError("ERR wrong number of arguments for 'flushdb' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		// Collect all regions.
		std::vector<std::shared_ptr<neokv::Region>> regions;
		neokv::Store::get_instance()->traverse_region_map(
		    [&regions](const neokv::SmartRegion& region) { regions.push_back(region); });

		if (regions.empty()) {
			output->SetStatus("OK");
			return brpc::REDIS_CMD_HANDLED;
		}

		// Send REDIS_FLUSHDB to each region through Raft.
		for (auto& region : regions) {
			neokv::pb::RedisWriteRequest redis_req;
			redis_req.set_cmd(neokv::pb::REDIS_FLUSHDB);
			redis_req.set_slot(0);

			neokv::pb::StoreRes response;
			int ret = region->exec_redis_write(redis_req, &response);
			if (ret != 0) {
				if (response.errcode() == neokv::pb::NOT_LEADER) {
					// In standalone mode this shouldn't happen, but handle gracefully.
					output->SetError("ERR FLUSHDB failed: not leader");
				} else {
					output->SetError(response.errmsg().c_str());
				}
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		output->SetStatus("OK");
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Phase 2 Read Command Handlers (EXISTS, PTTL, STRLEN, TYPE)
// ============================================================================

// Forward declaration (defined in Hash section below)
static int read_any_metadata(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                             neokv::RedisMetadata* meta);

class ExistsCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 2) {
			output->SetError("ERR wrong number of arguments for 'exists' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		if (!check_read_consistency(route, output)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t count = 0;
		for (size_t i = 1; i < args.size(); ++i) {
			std::string user_key(args[i].data(), args[i].size());
			int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, nullptr, nullptr);
			if (ret == 0) {
				++count;
			} else if (ret == 1) {
				// Also check metadata CF for non-string types (hash, etc.)
				neokv::RedisMetadata meta(neokv::kRedisNone, false);
				int meta_ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
				if (meta_ret == 0 && meta.type() != neokv::kRedisString) {
					++count;
				}
			}
		}

		output->SetInteger(count);
		return brpc::REDIS_CMD_HANDLED;
	}
};

class PttlCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'pttl' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		if (!check_read_consistency(route, output)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		int64_t expire_at_ms = 0;

		int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, nullptr, &expire_at_ms);

		if (ret == 1) {
			// Key not found
			output->SetInteger(-2);
		} else if (ret == 0) {
			if (expire_at_ms == neokv::REDIS_NO_EXPIRE) {
				output->SetInteger(-1);
			} else {
				int64_t remaining_ms = expire_at_ms - neokv::RedisCodec::current_time_ms();
				if (remaining_ms < 0) {
					remaining_ms = 0;
				}
				output->SetInteger(remaining_ms);
			}
		} else {
			output->SetError("ERR internal error");
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

class StrlenCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'strlen' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		if (!check_read_consistency(route, output)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		std::string value;

		int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, &value, nullptr);

		if (ret == 0) {
			output->SetInteger(static_cast<int64_t>(value.size()));
		} else if (ret == 1) {
			output->SetInteger(0);
		} else {
			output->SetError("ERR internal error");
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

class TypeCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'type' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		if (!check_read_consistency(route, output)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());

		int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, nullptr, nullptr);

		if (ret == 0) {
			output->SetStatus("string");
		} else if (ret == 1) {
			// Check if it's a different type in metadata CF
			neokv::RedisMetadata meta(neokv::kRedisNone, false);
			int meta_ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
			if (meta_ret == 0) {
				output->SetStatus(neokv::redis_type_name(meta.type()));
			} else {
				output->SetStatus("none");
			}
		} else {
			output->SetError("ERR internal error");
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Phase 2 Write Command Handlers (SETNX, SETEX, PSETEX, PERSIST, EXPIREAT,
//                                  PEXPIRE, PEXPIREAT, UNLINK)
// ============================================================================

class SetnxCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SETNX key value
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'setnx' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		std::string user_value(args[2].data(), args[2].size());

		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, user_value);

		int64_t affected = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_SET, kvs, neokv::REDIS_NO_EXPIRE, output, &affected,
		                             neokv::pb::REDIS_SET_CONDITION_NX);
		if (ret == 0) {
			// SETNX returns 1 if key was set, 0 if key already existed
			output->SetInteger(affected > 0 ? 1 : 0);
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

class SetexCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SETEX key seconds value
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'setex' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t seconds = 0;
		try {
			seconds = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (seconds <= 0) {
			output->SetError("ERR invalid expire time in 'setex' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		std::string user_value(args[3].data(), args[3].size());
		int64_t expire_at_ms = neokv::RedisCodec::current_time_ms() + seconds * 1000;

		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, user_value);

		int ret = write_through_raft(route, neokv::pb::REDIS_SET, kvs, expire_at_ms, output);
		if (ret == 0) {
			output->SetStatus("OK");
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

class PsetexCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// PSETEX key milliseconds value
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'psetex' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t milliseconds = 0;
		try {
			milliseconds = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (milliseconds <= 0) {
			output->SetError("ERR invalid expire time in 'psetex' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		std::string user_value(args[3].data(), args[3].size());
		int64_t expire_at_ms = neokv::RedisCodec::current_time_ms() + milliseconds;

		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, user_value);

		int ret = write_through_raft(route, neokv::pb::REDIS_SET, kvs, expire_at_ms, output);
		if (ret == 0) {
			output->SetStatus("OK");
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

class PersistCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// PERSIST key
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'persist' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, "");

		int64_t affected = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_PERSIST, kvs, neokv::REDIS_NO_EXPIRE, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected > 0 ? 1 : 0);
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

class ExpireatCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// EXPIREAT key timestamp
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'expireat' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t timestamp = 0;
		try {
			timestamp = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		// EXPIREAT takes a Unix timestamp in seconds, convert to ms
		int64_t expire_at_ms = timestamp * 1000;
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, "");

		int64_t affected = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_EXPIREAT, kvs, expire_at_ms, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected > 0 ? 1 : 0);
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

class PexpireCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// PEXPIRE key milliseconds
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'pexpire' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t milliseconds = 0;
		try {
			milliseconds = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		// PEXPIRE: relative TTL in milliseconds
		int64_t expire_at_ms = neokv::RedisCodec::current_time_ms() + milliseconds;
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, "");

		int64_t affected = 0;
		// Reuse REDIS_EXPIRE since the apply logic is the same (it just sets expire_ms)
		int ret = write_through_raft(route, neokv::pb::REDIS_EXPIRE, kvs, expire_at_ms, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected > 0 ? 1 : 0);
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

class PexpireatCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// PEXPIREAT key milliseconds-timestamp
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'pexpireat' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t timestamp_ms = 0;
		try {
			timestamp_ms = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route)) {
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string user_key(args[1].data(), args[1].size());
		// PEXPIREAT: absolute timestamp in milliseconds
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, "");

		int64_t affected = 0;
		// Reuse REDIS_EXPIREAT since the apply logic is the same
		int ret = write_through_raft(route, neokv::pb::REDIS_EXPIREAT, kvs, timestamp_ms, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected > 0 ? 1 : 0);
		}

		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Hash Command Helpers (added to RedisDataCommandHandler via free functions)
// ============================================================================

// Helper: write a hash command through Raft with field-value pairs
static int write_hash_through_raft(RedisDataCommandHandler* handler, const neokv::RedisRouteResult& route,
                                   neokv::pb::RedisCmd cmd, const std::string& user_key,
                                   const std::vector<std::pair<std::string, std::string>>& fields,
                                   brpc::RedisReply* output, int64_t* affected_count = nullptr,
                                   std::string* result_str = nullptr) {
	neokv::pb::RedisWriteRequest redis_req;
	redis_req.set_cmd(cmd);
	redis_req.set_slot(route.slot);

	// The hash key goes in kvs[0]
	auto* kv_pb = redis_req.add_kvs();
	kv_pb->set_key(user_key);

	// Field-value pairs go in the fields repeated field
	for (const auto& fv : fields) {
		auto* field_pb = redis_req.add_fields();
		field_pb->set_field(fv.first);
		if (!fv.second.empty() || cmd == neokv::pb::REDIS_HSET || cmd == neokv::pb::REDIS_HSETNX ||
		    cmd == neokv::pb::REDIS_HINCRBY || cmd == neokv::pb::REDIS_HINCRBYFLOAT) {
			field_pb->set_value(fv.second);
		}
	}

	if (route.region == nullptr) {
		output->SetError("CLUSTERDOWN Hash slot not served");
		return -1;
	}

	neokv::pb::StoreRes response;
	int ret = route.region->exec_redis_write(redis_req, &response);

	if (ret != 0) {
		if (response.errcode() == neokv::pb::NOT_LEADER) {
			handler->set_moved_error(route.slot, response.leader(), output);
		} else {
			output->SetError(response.errmsg().c_str());
		}
		return -1;
	}

	if (affected_count) {
		*affected_count = response.affected_rows();
	}
	if (result_str && response.has_errmsg() && !response.errmsg().empty()) {
		*result_str = response.errmsg();
	}
	return 0;
}

// Helper: read metadata for any key type from metadata CF
// Returns: 0 = found (not expired/dead), 1 = not found/expired, -1 = error
static int read_any_metadata(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                             neokv::RedisMetadata* meta) {
	auto* rocks = neokv::RocksWrapper::get_instance();
	if (rocks == nullptr)
		return -1;

	rocksdb::ReadOptions read_options;
	std::string meta_key = neokv::RedisMetadataKey::encode(region_id, index_id, slot, user_key);
	std::string raw_value;
	auto status = rocks->get(read_options, rocks->get_redis_metadata_handle(), meta_key, &raw_value);

	if (status.IsNotFound())
		return 1;
	if (!status.ok())
		return -1;

	neokv::RedisMetadata m(neokv::kRedisNone, false);
	const char* ptr = raw_value.data();
	size_t remaining = raw_value.size();
	if (!m.decode(&ptr, &remaining))
		return -1;
	if (m.is_dead())
		return 1;
	*meta = m;
	return 0;
}

// Helper: read a single hash field value
// Returns: 0 = found, 1 = not found, -1 = error
static int read_hash_field(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                           uint64_t version, const std::string& field, std::string* value) {
	auto* rocks = neokv::RocksWrapper::get_instance();
	if (rocks == nullptr)
		return -1;

	std::string subkey = neokv::RedisSubkeyKey::encode(region_id, index_id, slot, user_key, version, field);
	rocksdb::ReadOptions read_options;
	std::string raw_value;
	auto status = rocks->get(read_options, rocks->get_data_handle(), subkey, &raw_value);

	if (status.IsNotFound())
		return 1;
	if (!status.ok())
		return -1;
	if (value)
		*value = std::move(raw_value);
	return 0;
}

// ============================================================================
// Phase 3: Hash Read Command Handlers
// ============================================================================

class HGetCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'hget' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string field(args[2].data(), args[2].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisHash) {
			output->SetNullString();
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string value;
		ret = read_hash_field(route.region_id, get_index_id(route), route.slot, user_key, meta.version, field, &value);
		if (ret == 0) {
			output->SetString(value);
		} else {
			output->SetNullString();
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HMGetCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'hmget' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		size_t num_fields = args.size() - 2;

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		bool hash_exists = (ret == 0 && meta.type() == neokv::kRedisHash);

		output->SetArray(num_fields);
		for (size_t i = 0; i < num_fields; ++i) {
			std::string field(args[i + 2].data(), args[i + 2].size());
			if (hash_exists) {
				std::string value;
				int r = read_hash_field(route.region_id, get_index_id(route), route.slot, user_key, meta.version, field,
				                        &value);
				if (r == 0) {
					(*output)[i].SetString(value);
				} else {
					(*output)[i].SetNullString();
				}
			} else {
				(*output)[i].SetNullString();
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HGetAllCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'hgetall' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisHash) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		// Iterate all subkeys for this hash
		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                          user_key, meta.version);

		rocksdb::ReadOptions read_options;
		read_options.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

		std::vector<std::pair<std::string, std::string>> pairs;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;

			// Extract field name (everything after the prefix)
			std::string field_name(key.data() + prefix.size(), key.size() - prefix.size());
			std::string field_value(iter->value().data(), iter->value().size());
			pairs.emplace_back(std::move(field_name), std::move(field_value));
		}

		output->SetArray(pairs.size() * 2);
		for (size_t i = 0; i < pairs.size(); ++i) {
			(*output)[i * 2].SetString(pairs[i].first);
			(*output)[i * 2 + 1].SetString(pairs[i].second);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HKeysCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'hkeys' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisHash) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                          user_key, meta.version);

		rocksdb::ReadOptions read_options;
		read_options.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

		std::vector<std::string> keys;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;
			keys.emplace_back(key.data() + prefix.size(), key.size() - prefix.size());
		}

		output->SetArray(keys.size());
		for (size_t i = 0; i < keys.size(); ++i) {
			(*output)[i].SetString(keys[i]);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HValsCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'hvals' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisHash) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                          user_key, meta.version);

		rocksdb::ReadOptions read_options;
		read_options.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

		std::vector<std::string> vals;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;
			vals.emplace_back(iter->value().data(), iter->value().size());
		}

		output->SetArray(vals.size());
		for (size_t i = 0; i < vals.size(); ++i) {
			(*output)[i].SetString(vals[i]);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HLenCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'hlen' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisHash) {
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		output->SetInteger(static_cast<int64_t>(meta.size));
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HExistsCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'hexists' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string field(args[2].data(), args[2].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisHash) {
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		ret = read_hash_field(route.region_id, get_index_id(route), route.slot, user_key, meta.version, field, nullptr);
		output->SetInteger(ret == 0 ? 1 : 0);
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Phase 3: Hash Write Command Handlers
// ============================================================================

class HSetCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// HSET key field value [field value ...]
		if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
			output->SetError("ERR wrong number of arguments for 'hset' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> fields;
		for (size_t i = 2; i < args.size(); i += 2) {
			fields.emplace_back(std::string(args[i].data(), args[i].size()),
			                    std::string(args[i + 1].data(), args[i + 1].size()));
		}

		int64_t affected = 0;
		int ret = write_hash_through_raft(this, route, neokv::pb::REDIS_HSET, user_key, fields, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HMSetCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// HMSET key field value [field value ...]
		if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
			output->SetError("ERR wrong number of arguments for 'hmset' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> fields;
		for (size_t i = 2; i < args.size(); i += 2) {
			fields.emplace_back(std::string(args[i].data(), args[i].size()),
			                    std::string(args[i + 1].data(), args[i + 1].size()));
		}

		int64_t affected = 0;
		int ret = write_hash_through_raft(this, route, neokv::pb::REDIS_HSET, user_key, fields, output, &affected);
		if (ret == 0) {
			output->SetStatus("OK");
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HDelCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// HDEL key field [field ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'hdel' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> fields;
		for (size_t i = 2; i < args.size(); ++i) {
			fields.emplace_back(std::string(args[i].data(), args[i].size()), "");
		}

		int64_t affected = 0;
		int ret = write_hash_through_raft(this, route, neokv::pb::REDIS_HDEL, user_key, fields, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HSetNXCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// HSETNX key field value
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'hsetnx' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> fields;
		fields.emplace_back(std::string(args[2].data(), args[2].size()), std::string(args[3].data(), args[3].size()));

		int64_t affected = 0;
		int ret = write_hash_through_raft(this, route, neokv::pb::REDIS_HSETNX, user_key, fields, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HIncrByCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// HINCRBY key field increment
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'hincrby' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		// Validate increment is an integer
		std::string incr_str(args[3].data(), args[3].size());
		try {
			std::stoll(incr_str);
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> fields;
		fields.emplace_back(std::string(args[2].data(), args[2].size()), incr_str);

		int64_t affected = 0;
		int ret = write_hash_through_raft(this, route, neokv::pb::REDIS_HINCRBY, user_key, fields, output, &affected);
		if (ret == 0) {
			// affected_rows contains the new value for HINCRBY
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HIncrByFloatCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// HINCRBYFLOAT key field increment
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'hincrbyfloat' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string incr_str(args[3].data(), args[3].size());
		try {
			std::stod(incr_str);
		} catch (...) {
			output->SetError("ERR value is not a valid float");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> fields;
		fields.emplace_back(std::string(args[2].data(), args[2].size()), incr_str);

		int64_t affected = 0;
		std::string result_str;
		int ret = write_hash_through_raft(this, route, neokv::pb::REDIS_HINCRBYFLOAT, user_key, fields, output,
		                                  &affected, &result_str);
		if (ret == 0) {
			output->SetString(result_str);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Hash Read Command Handlers (continued)
// ============================================================================

// Simple glob pattern matching for HSCAN MATCH support.
// Handles '*' (match any sequence) and '?' (match single char).
static bool glob_match(const char* pattern, const char* str) {
	while (*pattern && *str) {
		if (*pattern == '*') {
			// Skip consecutive '*'
			while (*pattern == '*')
				++pattern;
			if (*pattern == '\0')
				return true;
			// Try matching the rest from every position
			while (*str) {
				if (glob_match(pattern, str))
					return true;
				++str;
			}
			return glob_match(pattern, str);
		} else if (*pattern == '?' || *pattern == *str) {
			++pattern;
			++str;
		} else {
			return false;
		}
	}
	// Consume trailing '*'
	while (*pattern == '*')
		++pattern;
	return *pattern == '\0' && *str == '\0';
}

class HRandFieldCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// HRANDFIELD key [count [WITHVALUES]]
		if (args.size() < 2 || args.size() > 4) {
			output->SetError("ERR wrong number of arguments for 'hrandfield' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		bool has_count = (args.size() >= 3);
		int64_t count = 1;
		bool allow_duplicates = false;
		bool with_values = false;

		if (has_count) {
			std::string count_str(args[2].data(), args[2].size());
			try {
				count = std::stoll(count_str);
			} catch (...) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
			if (count < 0) {
				allow_duplicates = true;
				count = -count;
			}
		}

		if (args.size() == 4) {
			std::string opt(args[3].data(), args[3].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), [](unsigned char c) { return std::tolower(c); });
			if (opt == "withvalues") {
				with_values = true;
			} else {
				output->SetError("ERR syntax error");
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		if (args.size() == 4 && !has_count) {
			output->SetError("ERR syntax error");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisHash) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		// Load all fields (and values if WITHVALUES)
		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                          user_key, meta.version);

		rocksdb::ReadOptions read_options;
		read_options.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

		std::vector<std::string> fields;
		std::vector<std::string> values;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;
			fields.emplace_back(key.data() + prefix.size(), key.size() - prefix.size());
			if (with_values) {
				values.emplace_back(iter->value().data(), iter->value().size());
			}
		}

		if (fields.empty()) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		if (count == 0 && has_count) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		std::mt19937 rng(std::random_device {}());

		if (!has_count) {
			// No count arg: return single random field name as bulk string
			std::uniform_int_distribution<size_t> dist(0, fields.size() - 1);
			output->SetString(fields[dist(rng)]);
			return brpc::REDIS_CMD_HANDLED;
		}

		if (allow_duplicates) {
			// Negative count: return abs(count) random fields, duplicates allowed
			size_t result_count = static_cast<size_t>(count);
			if (with_values) {
				output->SetArray(result_count * 2);
				std::uniform_int_distribution<size_t> dist(0, fields.size() - 1);
				for (size_t i = 0; i < result_count; ++i) {
					size_t idx = dist(rng);
					(*output)[i * 2].SetString(fields[idx]);
					(*output)[i * 2 + 1].SetString(values[idx]);
				}
			} else {
				output->SetArray(result_count);
				std::uniform_int_distribution<size_t> dist(0, fields.size() - 1);
				for (size_t i = 0; i < result_count; ++i) {
					(*output)[i].SetString(fields[dist(rng)]);
				}
			}
		} else {
			// Positive count: return min(count, size) unique fields
			size_t result_count = std::min(static_cast<size_t>(count), fields.size());
			// Fisher-Yates partial shuffle
			for (size_t i = 0; i < result_count; ++i) {
				std::uniform_int_distribution<size_t> dist(i, fields.size() - 1);
				size_t j = dist(rng);
				if (i != j) {
					std::swap(fields[i], fields[j]);
					if (with_values) {
						std::swap(values[i], values[j]);
					}
				}
			}
			if (with_values) {
				output->SetArray(result_count * 2);
				for (size_t i = 0; i < result_count; ++i) {
					(*output)[i * 2].SetString(fields[i]);
					(*output)[i * 2 + 1].SetString(values[i]);
				}
			} else {
				output->SetArray(result_count);
				for (size_t i = 0; i < result_count; ++i) {
					(*output)[i].SetString(fields[i]);
				}
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class HScanCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// HSCAN key cursor [MATCH pattern] [COUNT count]
		if (args.size() < 3 || args.size() > 7) {
			output->SetError("ERR wrong number of arguments for 'hscan' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string match_pattern;
		bool has_match = false;

		// Parse optional MATCH and COUNT arguments
		for (size_t i = 3; i < args.size(); i += 2) {
			if (i + 1 >= args.size()) {
				output->SetError("ERR syntax error");
				return brpc::REDIS_CMD_HANDLED;
			}
			std::string opt(args[i].data(), args[i].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), [](unsigned char c) { return std::tolower(c); });
			if (opt == "match") {
				match_pattern.assign(args[i + 1].data(), args[i + 1].size());
				has_match = true;
			} else if (opt == "count") {
				// COUNT is a hint; we ignore it and always return all matching fields
				std::string count_str(args[i + 1].data(), args[i + 1].size());
				try {
					int64_t cnt = std::stoll(count_str);
					if (cnt < 0) {
						output->SetError("ERR value is not an integer or out of range");
						return brpc::REDIS_CMD_HANDLED;
					}
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
			} else {
				output->SetError("ERR syntax error");
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisHash) {
			// Return [cursor="0", []] for non-existing key
			output->SetArray(2);
			(*output)[0].SetString("0");
			(*output)[1].SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		// Iterate all subkeys, collect field-value pairs
		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                          user_key, meta.version);

		rocksdb::ReadOptions read_options;
		read_options.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

		std::vector<std::pair<std::string, std::string>> pairs;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;

			std::string field_name(key.data() + prefix.size(), key.size() - prefix.size());

			// Apply MATCH filter if specified
			if (has_match && !glob_match(match_pattern.c_str(), field_name.c_str())) {
				continue;
			}

			std::string field_value(iter->value().data(), iter->value().size());
			pairs.emplace_back(std::move(field_name), std::move(field_value));
		}

		// Return [cursor="0", [field1, val1, field2, val2, ...]]
		output->SetArray(2);
		(*output)[0].SetString("0");
		(*output)[1].SetArray(pairs.size() * 2);
		for (size_t i = 0; i < pairs.size(); ++i) {
			(*output)[1][i * 2].SetString(pairs[i].first);
			(*output)[1][i * 2 + 1].SetString(pairs[i].second);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Set Command Helpers
// ============================================================================

// Helper: write a set command through Raft with members
static int write_set_through_raft(RedisDataCommandHandler* handler, const neokv::RedisRouteResult& route,
                                  neokv::pb::RedisCmd cmd, const std::string& user_key,
                                  const std::vector<std::string>& members, brpc::RedisReply* output,
                                  int64_t* affected_count = nullptr, std::string* result_str = nullptr,
                                  int64_t spop_count = 0) {
	neokv::pb::RedisWriteRequest redis_req;
	redis_req.set_cmd(cmd);
	redis_req.set_slot(route.slot);

	// The set key goes in kvs[0]
	auto* kv_pb = redis_req.add_kvs();
	kv_pb->set_key(user_key);
	// For SPOP, pass count via expire_ms (repurposed)
	if (cmd == neokv::pb::REDIS_SPOP && spop_count > 0) {
		kv_pb->set_expire_ms(spop_count);
	}

	// Members go in the members repeated field
	for (const auto& m : members) {
		redis_req.add_members(m);
	}

	if (route.region == nullptr) {
		output->SetError("CLUSTERDOWN Hash slot not served");
		return -1;
	}

	neokv::pb::StoreRes response;
	int ret = route.region->exec_redis_write(redis_req, &response);

	if (ret != 0) {
		if (response.errcode() == neokv::pb::NOT_LEADER) {
			handler->set_moved_error(route.slot, response.leader(), output);
		} else {
			output->SetError(response.errmsg().c_str());
		}
		return -1;
	}

	if (affected_count) {
		*affected_count = response.affected_rows();
	}
	if (result_str && response.has_errmsg() && !response.errmsg().empty()) {
		*result_str = response.errmsg();
	}
	return 0;
}

// ============================================================================
// Set Read Command Handlers
// ============================================================================

class SMembersCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'smembers' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisSet) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                          user_key, meta.version);

		rocksdb::ReadOptions read_options;
		read_options.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

		std::vector<std::string> members;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;
			members.emplace_back(key.data() + prefix.size(), key.size() - prefix.size());
		}

		output->SetArray(members.size());
		for (size_t i = 0; i < members.size(); ++i) {
			(*output)[i].SetString(members[i]);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SIsMemberCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'sismember' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string member(args[2].data(), args[2].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisSet) {
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		// Point lookup for the member subkey
		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string subkey = neokv::RedisSubkeyKey::encode(route.region_id, get_index_id(route), route.slot, user_key,
		                                                   meta.version, member);
		rocksdb::ReadOptions read_options;
		std::string dummy;
		auto status = rocks->get(read_options, rocks->get_data_handle(), subkey, &dummy);

		output->SetInteger(status.ok() ? 1 : 0);
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SCardCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'scard' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisSet) {
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		output->SetInteger(static_cast<int64_t>(meta.size));
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SRandMemberCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 2 || args.size() > 3) {
			output->SetError("ERR wrong number of arguments for 'srandmember' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		bool has_count = (args.size() == 3);
		int64_t count = 1;
		bool allow_duplicates = false;
		if (has_count) {
			std::string count_str(args[2].data(), args[2].size());
			try {
				count = std::stoll(count_str);
			} catch (...) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
			if (count < 0) {
				allow_duplicates = true;
				count = -count;
			}
		}

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisSet) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		// Load all members
		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                          user_key, meta.version);
		rocksdb::ReadOptions read_options;
		read_options.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

		std::vector<std::string> all_members;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;
			all_members.emplace_back(key.data() + prefix.size(), key.size() - prefix.size());
		}

		if (all_members.empty()) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		if (count == 0) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		std::mt19937 rng(std::random_device {}());

		if (!has_count) {
			// No count arg: return single bulk string
			std::uniform_int_distribution<size_t> dist(0, all_members.size() - 1);
			output->SetString(all_members[dist(rng)]);
			return brpc::REDIS_CMD_HANDLED;
		}

		if (allow_duplicates) {
			// Negative count: return count elements, duplicates allowed
			output->SetArray(count);
			std::uniform_int_distribution<size_t> dist(0, all_members.size() - 1);
			for (int64_t i = 0; i < count; ++i) {
				(*output)[i].SetString(all_members[dist(rng)]);
			}
		} else {
			// Positive count: return min(count, size) unique elements
			size_t result_count = std::min(static_cast<size_t>(count), all_members.size());
			// Fisher-Yates partial shuffle
			for (size_t i = 0; i < result_count; ++i) {
				std::uniform_int_distribution<size_t> dist(i, all_members.size() - 1);
				size_t j = dist(rng);
				if (i != j)
					std::swap(all_members[i], all_members[j]);
			}
			output->SetArray(result_count);
			for (size_t i = 0; i < result_count; ++i) {
				(*output)[i].SetString(all_members[i]);
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Set Write Command Handlers
// ============================================================================

class SAddCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SADD key member [member ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'sadd' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::string> members;
		for (size_t i = 2; i < args.size(); ++i) {
			members.emplace_back(args[i].data(), args[i].size());
		}

		int64_t affected = 0;
		int ret = write_set_through_raft(this, route, neokv::pb::REDIS_SADD, user_key, members, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SRemCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SREM key member [member ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'srem' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::string> members;
		for (size_t i = 2; i < args.size(); ++i) {
			members.emplace_back(args[i].data(), args[i].size());
		}

		int64_t affected = 0;
		int ret = write_set_through_raft(this, route, neokv::pb::REDIS_SREM, user_key, members, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SPopCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SPOP key [count]
		if (args.size() < 2 || args.size() > 3) {
			output->SetError("ERR wrong number of arguments for 'spop' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		bool has_count = (args.size() == 3);
		int64_t count = 1;
		if (has_count) {
			std::string count_str(args[2].data(), args[2].size());
			try {
				count = std::stoll(count_str);
			} catch (...) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
			if (count < 0) {
				output->SetError("ERR index out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		if (count == 0) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		std::vector<std::string> empty_members; // SPOP doesn't send members, count is in kvs[0].expire_ms
		int64_t affected = 0;
		std::string result_str;
		int ret = write_set_through_raft(this, route, neokv::pb::REDIS_SPOP, user_key, empty_members, output, &affected,
		                                 &result_str, count);
		if (ret == 0) {
			if (affected == 0) {
				if (has_count) {
					output->SetArray(0);
				} else {
					output->SetNullString();
				}
			} else if (!has_count) {
				// Single pop — return bulk string
				output->SetString(result_str);
			} else {
				// Multiple pop — parse newline-separated members
				std::vector<std::string> popped;
				size_t start = 0;
				for (size_t i = 0; i <= result_str.size(); ++i) {
					if (i == result_str.size() || result_str[i] == '\n') {
						if (i > start) {
							popped.emplace_back(result_str.substr(start, i - start));
						}
						start = i + 1;
					}
				}
				output->SetArray(popped.size());
				for (size_t i = 0; i < popped.size(); ++i) {
					(*output)[i].SetString(popped[i]);
				}
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Set Additional Read Command Handlers
// ============================================================================

// Helper: load all members of a set into a vector
// Returns: 0 = success (members populated), 1 = key not found or not a set, -1 = error
static int load_set_members(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                            std::vector<std::string>* members) {
	neokv::RedisMetadata meta(neokv::kRedisNone, false);
	int ret = read_any_metadata(region_id, index_id, slot, user_key, &meta);
	if (ret != 0 || meta.type() != neokv::kRedisSet) {
		return 1;
	}

	auto* rocks = neokv::RocksWrapper::get_instance();
	std::string prefix = neokv::RedisSubkeyKey::encode_prefix(region_id, index_id, slot, user_key, meta.version);

	rocksdb::ReadOptions read_options;
	read_options.prefix_same_as_start = true;
	std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

	for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
		rocksdb::Slice key = iter->key();
		if (!key.starts_with(prefix))
			break;
		members->emplace_back(key.data() + prefix.size(), key.size() - prefix.size());
	}
	return 0;
}

// Helper: load set members into an unordered_set
static int load_set_members_set(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                                std::unordered_set<std::string>* member_set) {
	std::vector<std::string> members;
	int ret = load_set_members(region_id, index_id, slot, user_key, &members);
	if (ret != 0)
		return ret;
	for (auto& m : members) {
		member_set->insert(std::move(m));
	}
	return 0;
}

class SMisMemberCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SMISMEMBER key member [member ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'smismember' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		bool key_exists = (ret == 0 && meta.type() == neokv::kRedisSet);

		size_t member_count = args.size() - 2;
		output->SetArray(member_count);

		if (!key_exists) {
			for (size_t i = 0; i < member_count; ++i) {
				(*output)[i].SetInteger(0);
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		auto* rocks = neokv::RocksWrapper::get_instance();
		for (size_t i = 0; i < member_count; ++i) {
			std::string member(args[i + 2].data(), args[i + 2].size());
			std::string subkey = neokv::RedisSubkeyKey::encode(route.region_id, get_index_id(route), route.slot,
			                                                   user_key, meta.version, member);
			rocksdb::ReadOptions read_options;
			std::string dummy;
			auto status = rocks->get(read_options, rocks->get_data_handle(), subkey, &dummy);
			(*output)[i].SetInteger(status.ok() ? 1 : 0);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SScanCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SSCAN key cursor [MATCH pattern] [COUNT count]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'sscan' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		// cursor is args[2] — we ignore it (return all in one batch, cursor=0)

		// Parse optional MATCH and COUNT
		bool has_match = false;
		std::string match_pattern;
		for (size_t i = 3; i + 1 < args.size(); i += 2) {
			std::string opt(args[i].data(), args[i].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
			if (opt == "MATCH") {
				has_match = true;
				match_pattern.assign(args[i + 1].data(), args[i + 1].size());
			}
		}

		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisSet) {
			// Return ["0", []]
			output->SetArray(2);
			(*output)[0].SetString("0");
			(*output)[1].SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisSubkeyKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                          user_key, meta.version);
		rocksdb::ReadOptions read_options;
		read_options.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(read_options, rocks->get_data_handle()));

		std::vector<std::string> members;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;
			std::string member(key.data() + prefix.size(), key.size() - prefix.size());
			if (has_match && !glob_match(match_pattern.c_str(), member.c_str()))
				continue;
			members.push_back(std::move(member));
		}

		// Return ["0", [member1, member2, ...]]
		output->SetArray(2);
		(*output)[0].SetString("0");
		(*output)[1].SetArray(members.size());
		for (size_t i = 0; i < members.size(); ++i) {
			(*output)[1][i].SetString(members[i]);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SInterCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SINTER key [key ...]
		if (args.size() < 2) {
			output->SetError("ERR wrong number of arguments for 'sinter' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		// Load first set
		std::string first_key(args[1].data(), args[1].size());
		std::unordered_set<std::string> result;
		int ret = load_set_members_set(route.region_id, get_index_id(route), route.slot, first_key, &result);
		if (ret != 0 || result.empty()) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		// Intersect with remaining sets
		for (size_t i = 2; i < args.size() && !result.empty(); ++i) {
			std::string key(args[i].data(), args[i].size());
			std::unordered_set<std::string> other;
			ret = load_set_members_set(route.region_id, get_index_id(route), route.slot, key, &other);
			if (ret != 0) {
				// Key doesn't exist = empty set, intersection is empty
				output->SetArray(0);
				return brpc::REDIS_CMD_HANDLED;
			}
			// Remove from result anything not in other
			for (auto it = result.begin(); it != result.end();) {
				if (other.find(*it) == other.end()) {
					it = result.erase(it);
				} else {
					++it;
				}
			}
		}

		std::vector<std::string> sorted_result(result.begin(), result.end());
		std::sort(sorted_result.begin(), sorted_result.end());
		output->SetArray(sorted_result.size());
		for (size_t i = 0; i < sorted_result.size(); ++i) {
			(*output)[i].SetString(sorted_result[i]);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SUnionCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SUNION key [key ...]
		if (args.size() < 2) {
			output->SetError("ERR wrong number of arguments for 'sunion' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::unordered_set<std::string> result;
		for (size_t i = 1; i < args.size(); ++i) {
			std::string key(args[i].data(), args[i].size());
			std::vector<std::string> members;
			load_set_members(route.region_id, get_index_id(route), route.slot, key, &members);
			for (auto& m : members) {
				result.insert(std::move(m));
			}
		}

		std::vector<std::string> sorted_result(result.begin(), result.end());
		std::sort(sorted_result.begin(), sorted_result.end());
		output->SetArray(sorted_result.size());
		for (size_t i = 0; i < sorted_result.size(); ++i) {
			(*output)[i].SetString(sorted_result[i]);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SDiffCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SDIFF key [key ...]
		if (args.size() < 2) {
			output->SetError("ERR wrong number of arguments for 'sdiff' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		// Load first set
		std::string first_key(args[1].data(), args[1].size());
		std::unordered_set<std::string> result;
		load_set_members_set(route.region_id, get_index_id(route), route.slot, first_key, &result);
		if (result.empty()) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		// Remove members found in any subsequent set
		for (size_t i = 2; i < args.size() && !result.empty(); ++i) {
			std::string key(args[i].data(), args[i].size());
			std::vector<std::string> members;
			load_set_members(route.region_id, get_index_id(route), route.slot, key, &members);
			for (const auto& m : members) {
				result.erase(m);
			}
		}

		std::vector<std::string> sorted_result(result.begin(), result.end());
		std::sort(sorted_result.begin(), sorted_result.end());
		output->SetArray(sorted_result.size());
		for (size_t i = 0; i < sorted_result.size(); ++i) {
			(*output)[i].SetString(sorted_result[i]);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SInterCardCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SINTERCARD numkeys key [key ...] [LIMIT limit]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'sintercard' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string numkeys_str(args[1].data(), args[1].size());
		int64_t numkeys;
		try {
			numkeys = std::stoll(numkeys_str);
		} catch (...) {
			output->SetError("ERR numkeys for 'sintercard' command must be positive");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (numkeys <= 0) {
			output->SetError("ERR numkeys for 'sintercard' command must be positive");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (static_cast<size_t>(numkeys) + 2 > args.size()) {
			output->SetError("ERR Number of keys can't be greater than number of args");
			return brpc::REDIS_CMD_HANDLED;
		}

		// Parse optional LIMIT
		int64_t limit = 0; // 0 means no limit
		size_t keys_end = 2 + numkeys;
		for (size_t i = keys_end; i + 1 < args.size(); i += 2) {
			std::string opt(args[i].data(), args[i].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
			if (opt == "LIMIT") {
				std::string limit_str(args[i + 1].data(), args[i + 1].size());
				try {
					limit = std::stoll(limit_str);
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (limit < 0) {
					output->SetError("ERR LIMIT can't be negative");
					return brpc::REDIS_CMD_HANDLED;
				}
			}
		}

		// Route by first key — SINTERCARD uses {0,0,0} routing so we must
		// manually route using the first key (args[2])
		std::string first_key_for_route(args[2].data(), args[2].size());
		auto* router = neokv::RedisRouter::get_instance();
		uint16_t slot = router->calc_slot(first_key_for_route);
		int64_t region_id = 0;
		auto region = router->find_region_by_slot(slot, &region_id);

		neokv::RedisRouteResult route;
		route.status = neokv::RedisRouteResult::OK;
		route.slot = slot;
		route.region_id = region_id;
		route.region = region;
		if (region != nullptr) {
			route.table_id = region->get_table_id();
			route.index_id = region->get_table_id();
		}
		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		// Load first set
		std::string first_key(args[2].data(), args[2].size());
		std::unordered_set<std::string> result;
		int ret = load_set_members_set(route.region_id, get_index_id(route), route.slot, first_key, &result);
		if (ret != 0 || result.empty()) {
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		// Intersect with remaining sets
		for (int64_t i = 1; i < numkeys && !result.empty(); ++i) {
			std::string key(args[2 + i].data(), args[2 + i].size());
			std::unordered_set<std::string> other;
			ret = load_set_members_set(route.region_id, get_index_id(route), route.slot, key, &other);
			if (ret != 0) {
				output->SetInteger(0);
				return brpc::REDIS_CMD_HANDLED;
			}
			for (auto it = result.begin(); it != result.end();) {
				if (other.find(*it) == other.end()) {
					it = result.erase(it);
				} else {
					++it;
				}
			}
		}

		int64_t count = static_cast<int64_t>(result.size());
		if (limit > 0 && count > limit) {
			count = limit;
		}
		output->SetInteger(count);
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Set Additional Write Command Handlers
// ============================================================================

class SMoveCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SMOVE source destination member
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'smove' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string source(args[1].data(), args[1].size());
		std::string destination(args[2].data(), args[2].size());
		std::string member(args[3].data(), args[3].size());

		// kvs[0].key = source, kvs[0].value = destination
		// members[0] = member to move
		std::vector<std::string> members = {member};
		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(neokv::pb::REDIS_SMOVE);
		redis_req.set_slot(route.slot);
		auto* kv_pb = redis_req.add_kvs();
		kv_pb->set_key(source);
		kv_pb->set_value(destination);
		redis_req.add_members(member);

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);

		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER) {
				set_moved_error(route.slot, response.leader(), output);
			} else {
				output->SetError(response.errmsg().c_str());
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		output->SetInteger(response.affected_rows());
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SInterStoreCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SINTERSTORE destination key [key ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'sinterstore' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string dest_key(args[1].data(), args[1].size());

		// Compute intersection on the read side
		std::string first_key(args[2].data(), args[2].size());
		std::unordered_set<std::string> result;
		int ret = load_set_members_set(route.region_id, get_index_id(route), route.slot, first_key, &result);
		if (ret != 0) {
			result.clear();
		}

		if (!result.empty()) {
			for (size_t i = 3; i < args.size() && !result.empty(); ++i) {
				std::string key(args[i].data(), args[i].size());
				std::unordered_set<std::string> other;
				ret = load_set_members_set(route.region_id, get_index_id(route), route.slot, key, &other);
				if (ret != 0) {
					result.clear();
					break;
				}
				for (auto it = result.begin(); it != result.end();) {
					if (other.find(*it) == other.end()) {
						it = result.erase(it);
					} else {
						++it;
					}
				}
			}
		}

		// Write result to destination via Raft
		std::vector<std::string> members(result.begin(), result.end());
		int64_t affected = 0;
		int wret =
		    write_set_through_raft(this, route, neokv::pb::REDIS_SINTERSTORE, dest_key, members, output, &affected);
		if (wret == 0) {
			output->SetInteger(static_cast<int64_t>(members.size()));
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SUnionStoreCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SUNIONSTORE destination key [key ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'sunionstore' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string dest_key(args[1].data(), args[1].size());

		// Compute union on the read side
		std::unordered_set<std::string> result;
		for (size_t i = 2; i < args.size(); ++i) {
			std::string key(args[i].data(), args[i].size());
			std::vector<std::string> members;
			load_set_members(route.region_id, get_index_id(route), route.slot, key, &members);
			for (auto& m : members) {
				result.insert(std::move(m));
			}
		}

		// Write result to destination via Raft
		std::vector<std::string> members(result.begin(), result.end());
		int64_t affected = 0;
		int wret =
		    write_set_through_raft(this, route, neokv::pb::REDIS_SUNIONSTORE, dest_key, members, output, &affected);
		if (wret == 0) {
			output->SetInteger(static_cast<int64_t>(members.size()));
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SDiffStoreCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SDIFFSTORE destination key [key ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'sdiffstore' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string dest_key(args[1].data(), args[1].size());

		// Compute difference on the read side
		std::string first_key(args[2].data(), args[2].size());
		std::unordered_set<std::string> result;
		load_set_members_set(route.region_id, get_index_id(route), route.slot, first_key, &result);

		if (!result.empty()) {
			for (size_t i = 3; i < args.size() && !result.empty(); ++i) {
				std::string key(args[i].data(), args[i].size());
				std::vector<std::string> members;
				load_set_members(route.region_id, get_index_id(route), route.slot, key, &members);
				for (const auto& m : members) {
					result.erase(m);
				}
			}
		}

		// Write result to destination via Raft
		std::vector<std::string> members(result.begin(), result.end());
		int64_t affected = 0;
		int wret =
		    write_set_through_raft(this, route, neokv::pb::REDIS_SDIFFSTORE, dest_key, members, output, &affected);
		if (wret == 0) {
			output->SetInteger(static_cast<int64_t>(members.size()));
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// Sorted Set (ZSet) Helpers
// ============================================================================

// Helper: write a ZSet command through Raft
static int write_zset_through_raft(RedisDataCommandHandler* handler, const neokv::RedisRouteResult& route,
                                   neokv::pb::RedisCmd cmd, const std::string& user_key, brpc::RedisReply* output,
                                   neokv::pb::RedisWriteRequest* redis_req, int64_t* affected_count = nullptr,
                                   std::string* result_str = nullptr) {
	redis_req->set_cmd(cmd);
	redis_req->set_slot(route.slot);
	auto* kv_pb = redis_req->add_kvs();
	kv_pb->set_key(user_key);

	if (route.region == nullptr) {
		output->SetError("CLUSTERDOWN Hash slot not served");
		return -1;
	}

	neokv::pb::StoreRes response;
	int ret = route.region->exec_redis_write(*redis_req, &response);
	if (ret != 0) {
		if (response.errcode() == neokv::pb::NOT_LEADER) {
			handler->set_moved_error(route.slot, response.leader(), output);
		} else {
			output->SetError(response.errmsg().c_str());
		}
		return -1;
	}
	if (affected_count)
		*affected_count = response.affected_rows();
	if (result_str && response.has_errmsg() && !response.errmsg().empty()) {
		*result_str = response.errmsg();
	}
	return 0;
}

// Helper: parse Redis score range string (e.g., "-inf", "+inf", "(1.5", "1.5")
// Returns true if parsed successfully. Sets exclusive flag.
static bool parse_score_range(const std::string& s, double* score, bool* exclusive) {
	*exclusive = false;
	if (s == "-inf" || s == "-INF") {
		*score = -std::numeric_limits<double>::infinity();
		return true;
	}
	if (s == "+inf" || s == "+INF" || s == "inf" || s == "INF") {
		*score = std::numeric_limits<double>::infinity();
		return true;
	}
	std::string val = s;
	if (!val.empty() && val[0] == '(') {
		*exclusive = true;
		val = val.substr(1);
	}
	try {
		*score = std::stod(val);
		return true;
	} catch (...) {
		return false;
	}
}

// PLACEHOLDER_ZSET_HANDLERS_1

class ZScoreCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'zscore' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		std::string member(args[2].data(), args[2].size());
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetNullString();
			return brpc::REDIS_CMD_HANDLED;
		}
		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string subkey = neokv::RedisSubkeyKey::encode(route.region_id, get_index_id(route), route.slot, user_key,
		                                                   meta.version, member);
		rocksdb::ReadOptions ro;
		std::string score_bytes;
		auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &score_bytes);
		if (!s.ok() || score_bytes.size() < 8) {
			output->SetNullString();
			return brpc::REDIS_CMD_HANDLED;
		}
		double score = neokv::RedisZSetScoreKey::decode_double(score_bytes.data());
		char buf[64];
		snprintf(buf, sizeof(buf), "%.17g", score);
		output->SetString(buf);
		return brpc::REDIS_CMD_HANDLED;
	}
};

class ZCardCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'zcard' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		output->SetInteger(static_cast<int64_t>(meta.size));
		return brpc::REDIS_CMD_HANDLED;
	}
};

// PLACEHOLDER_ZSET_HANDLERS_2

class ZRankCommandHandler final : public RedisDataCommandHandler {
public:
	bool reverse_;
	explicit ZRankCommandHandler(bool reverse = false) : reverse_(reverse) {
	}
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		std::string member(args[2].data(), args[2].size());
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetNullString();
			return brpc::REDIS_CMD_HANDLED;
		}
		// First check member exists and get its score
		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string subkey = neokv::RedisSubkeyKey::encode(route.region_id, get_index_id(route), route.slot, user_key,
		                                                   meta.version, member);
		rocksdb::ReadOptions ro;
		std::string score_bytes;
		auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &score_bytes);
		if (!s.ok() || score_bytes.size() < 8) {
			output->SetNullString();
			return brpc::REDIS_CMD_HANDLED;
		}
		// Iterate score CF to find rank
		std::string prefix = neokv::RedisZSetScoreKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                             user_key, meta.version);
		ro.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(ro, rocks->get_redis_zset_score_handle()));
		int64_t rank = 0;
		double target_score = neokv::RedisZSetScoreKey::decode_double(score_bytes.data());
		if (!reverse_) {
			for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
				rocksdb::Slice key = iter->key();
				if (!key.starts_with(prefix))
					break;
				// Extract member from score key: prefix + 8 bytes score + member
				size_t member_offset = prefix.size() + 8;
				if (key.size() >= member_offset) {
					std::string cur_member(key.data() + member_offset, key.size() - member_offset);
					if (cur_member == member) {
						output->SetInteger(rank);
						return brpc::REDIS_CMD_HANDLED;
					}
				}
				++rank;
			}
		} else {
			// For reverse rank, count total then subtract
			std::vector<std::string> all_members;
			for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
				rocksdb::Slice key = iter->key();
				if (!key.starts_with(prefix))
					break;
				size_t member_offset = prefix.size() + 8;
				if (key.size() >= member_offset) {
					all_members.emplace_back(key.data() + member_offset, key.size() - member_offset);
				}
			}
			for (int64_t i = static_cast<int64_t>(all_members.size()) - 1; i >= 0; --i) {
				if (all_members[i] == member) {
					output->SetInteger(static_cast<int64_t>(all_members.size()) - 1 - i);
					return brpc::REDIS_CMD_HANDLED;
				}
			}
		}
		output->SetNullString();
		return brpc::REDIS_CMD_HANDLED;
	}
};

// PLACEHOLDER_ZSET_HANDLERS_3

class ZCountCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'zcount' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		std::string min_str(args[2].data(), args[2].size());
		std::string max_str(args[3].data(), args[3].size());
		double min_score, max_score;
		bool min_ex, max_ex;
		if (!parse_score_range(min_str, &min_score, &min_ex) || !parse_score_range(max_str, &max_score, &max_ex)) {
			output->SetError("ERR min or max is not a float");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		auto* rocks = neokv::RocksWrapper::get_instance();
		std::string prefix = neokv::RedisZSetScoreKey::encode_prefix(route.region_id, get_index_id(route), route.slot,
		                                                             user_key, meta.version);
		rocksdb::ReadOptions ro;
		ro.prefix_same_as_start = true;
		std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(ro, rocks->get_redis_zset_score_handle()));
		int64_t count = 0;
		for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
			rocksdb::Slice key = iter->key();
			if (!key.starts_with(prefix))
				break;
			double score = neokv::RedisZSetScoreKey::decode_double(key.data() + prefix.size());
			if (min_ex ? score <= min_score : score < min_score)
				continue;
			if (max_ex ? score >= max_score : score > max_score)
				break;
			++count;
		}
		output->SetInteger(count);
		return brpc::REDIS_CMD_HANDLED;
	}
};

// PLACEHOLDER_ZSET_HANDLERS_4

// Helper: collect all (score, member) pairs from score CF, sorted by score ascending
static int load_zset_score_ordered(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                                   uint64_t version, std::vector<std::pair<double, std::string>>* entries) {
	auto* rocks = neokv::RocksWrapper::get_instance();
	std::string prefix = neokv::RedisZSetScoreKey::encode_prefix(region_id, index_id, slot, user_key, version);
	rocksdb::ReadOptions ro;
	ro.prefix_same_as_start = true;
	std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(ro, rocks->get_redis_zset_score_handle()));
	for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
		rocksdb::Slice key = iter->key();
		if (!key.starts_with(prefix))
			break;
		double score = neokv::RedisZSetScoreKey::decode_double(key.data() + prefix.size());
		std::string member(key.data() + prefix.size() + 8, key.size() - prefix.size() - 8);
		entries->emplace_back(score, std::move(member));
	}
	return 0;
}

class ZRangeCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
		if (args.size() < 4) {
			output->SetError("ERR wrong number of arguments for 'zrange' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		std::string min_str(args[2].data(), args[2].size());
		std::string max_str(args[3].data(), args[3].size());
		bool by_score = false, by_lex = false, rev = false, with_scores = false;
		int64_t limit_offset = 0, limit_count = -1;
		for (size_t i = 4; i < args.size(); ++i) {
			std::string opt(args[i].data(), args[i].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
			if (opt == "BYSCORE")
				by_score = true;
			else if (opt == "BYLEX")
				by_lex = true;
			else if (opt == "REV")
				rev = true;
			else if (opt == "WITHSCORES")
				with_scores = true;
			else if (opt == "LIMIT" && i + 2 < args.size()) {
				try {
					limit_offset = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
					limit_count = std::stoll(std::string(args[i + 2].data(), args[i + 2].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				i += 2;
			}
		}
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		std::vector<std::pair<double, std::string>> all_entries;
		load_zset_score_ordered(route.region_id, get_index_id(route), route.slot, user_key, meta.version, &all_entries);
		if (rev)
			std::reverse(all_entries.begin(), all_entries.end());
		std::vector<std::pair<double, std::string>> result;
		if (by_score) {
			double min_s, max_s;
			bool min_ex, max_ex;
			if (rev) {
				if (!parse_score_range(max_str, &min_s, &min_ex) || !parse_score_range(min_str, &max_s, &max_ex)) {
					output->SetError("ERR min or max is not a float");
					return brpc::REDIS_CMD_HANDLED;
				}
			} else {
				if (!parse_score_range(min_str, &min_s, &min_ex) || !parse_score_range(max_str, &max_s, &max_ex)) {
					output->SetError("ERR min or max is not a float");
					return brpc::REDIS_CMD_HANDLED;
				}
			}
			for (auto& e : all_entries) {
				double s = e.first;
				if (min_ex ? s <= min_s : s < min_s)
					continue;
				if (max_ex ? s >= max_s : s > max_s)
					continue;
				result.push_back(e);
			}
		} else if (by_lex) {
			// Lex range: all members must have same score. Filter by lex range.
			for (auto& e : all_entries)
				result.push_back(e);
			// TODO: implement proper lex filtering
		} else {
			// By rank (index)
			int64_t start, stop;
			try {
				start = std::stoll(min_str);
				stop = std::stoll(max_str);
			} catch (...) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
			int64_t sz = static_cast<int64_t>(all_entries.size());
			if (start < 0)
				start += sz;
			if (stop < 0)
				stop += sz;
			if (start < 0)
				start = 0;
			if (stop >= sz)
				stop = sz - 1;
			for (int64_t i = start; i <= stop; ++i)
				result.push_back(all_entries[i]);
		}
		// Apply LIMIT
		if ((by_score || by_lex) && limit_count >= 0) {
			if (limit_offset >= static_cast<int64_t>(result.size())) {
				result.clear();
			} else {
				result = std::vector<std::pair<double, std::string>>(
				    result.begin() + limit_offset,
				    (limit_count > 0 && limit_offset + limit_count < static_cast<int64_t>(result.size()))
				        ? result.begin() + limit_offset + limit_count
				        : result.end());
			}
		}
		if (with_scores) {
			output->SetArray(result.size() * 2);
			for (size_t i = 0; i < result.size(); ++i) {
				(*output)[i * 2].SetString(result[i].second);
				char buf[64];
				snprintf(buf, sizeof(buf), "%.17g", result[i].first);
				(*output)[i * 2 + 1].SetString(buf);
			}
		} else {
			output->SetArray(result.size());
			for (size_t i = 0; i < result.size(); ++i)
				(*output)[i].SetString(result[i].second);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// PLACEHOLDER_ZSET_HANDLERS_5

class ZRangeByScoreCommandHandler final : public RedisDataCommandHandler {
public:
	bool reverse_;
	explicit ZRangeByScoreCommandHandler(bool reverse = false) : reverse_(reverse) {
	}
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 4) {
			output->SetError("ERR wrong number of arguments");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		std::string arg2(args[2].data(), args[2].size());
		std::string arg3(args[3].data(), args[3].size());
		double min_s, max_s;
		bool min_ex, max_ex;
		if (reverse_) {
			if (!parse_score_range(arg2, &max_s, &max_ex) || !parse_score_range(arg3, &min_s, &min_ex)) {
				output->SetError("ERR min or max is not a float");
				return brpc::REDIS_CMD_HANDLED;
			}
		} else {
			if (!parse_score_range(arg2, &min_s, &min_ex) || !parse_score_range(arg3, &max_s, &max_ex)) {
				output->SetError("ERR min or max is not a float");
				return brpc::REDIS_CMD_HANDLED;
			}
		}
		bool with_scores = false;
		int64_t limit_offset = 0, limit_count = -1;
		for (size_t i = 4; i < args.size(); ++i) {
			std::string opt(args[i].data(), args[i].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
			if (opt == "WITHSCORES")
				with_scores = true;
			else if (opt == "LIMIT" && i + 2 < args.size()) {
				try {
					limit_offset = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
					limit_count = std::stoll(std::string(args[i + 2].data(), args[i + 2].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				i += 2;
			}
		}
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		std::vector<std::pair<double, std::string>> all_entries;
		load_zset_score_ordered(route.region_id, get_index_id(route), route.slot, user_key, meta.version, &all_entries);
		if (reverse_)
			std::reverse(all_entries.begin(), all_entries.end());
		std::vector<std::pair<double, std::string>> result;
		for (auto& e : all_entries) {
			double s = e.first;
			if (min_ex ? s <= min_s : s < min_s)
				continue;
			if (max_ex ? s >= max_s : s > max_s)
				continue;
			result.push_back(e);
		}
		if (limit_count >= 0) {
			if (limit_offset >= static_cast<int64_t>(result.size()))
				result.clear();
			else
				result = std::vector<std::pair<double, std::string>>(
				    result.begin() + limit_offset,
				    (limit_count > 0 && limit_offset + limit_count < static_cast<int64_t>(result.size()))
				        ? result.begin() + limit_offset + limit_count
				        : result.end());
		}
		if (with_scores) {
			output->SetArray(result.size() * 2);
			for (size_t i = 0; i < result.size(); ++i) {
				(*output)[i * 2].SetString(result[i].second);
				char buf[64];
				snprintf(buf, sizeof(buf), "%.17g", result[i].first);
				(*output)[i * 2 + 1].SetString(buf);
			}
		} else {
			output->SetArray(result.size());
			for (size_t i = 0; i < result.size(); ++i)
				(*output)[i].SetString(result[i].second);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class ZRevRangeCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 4) {
			output->SetError("ERR wrong number of arguments for 'zrevrange' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		bool with_scores = false;
		for (size_t i = 4; i < args.size(); ++i) {
			std::string opt(args[i].data(), args[i].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
			if (opt == "WITHSCORES")
				with_scores = true;
		}
		int64_t start, stop;
		try {
			start = std::stoll(std::string(args[2].data(), args[2].size()));
			stop = std::stoll(std::string(args[3].data(), args[3].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		std::vector<std::pair<double, std::string>> all_entries;
		load_zset_score_ordered(route.region_id, get_index_id(route), route.slot, user_key, meta.version, &all_entries);
		std::reverse(all_entries.begin(), all_entries.end());
		int64_t sz = static_cast<int64_t>(all_entries.size());
		if (start < 0)
			start += sz;
		if (stop < 0)
			stop += sz;
		if (start < 0)
			start = 0;
		if (stop >= sz)
			stop = sz - 1;
		std::vector<std::pair<double, std::string>> result;
		for (int64_t i = start; i <= stop; ++i)
			result.push_back(all_entries[i]);
		if (with_scores) {
			output->SetArray(result.size() * 2);
			for (size_t i = 0; i < result.size(); ++i) {
				(*output)[i * 2].SetString(result[i].second);
				char buf[64];
				snprintf(buf, sizeof(buf), "%.17g", result[i].first);
				(*output)[i * 2 + 1].SetString(buf);
			}
		} else {
			output->SetArray(result.size());
			for (size_t i = 0; i < result.size(); ++i)
				(*output)[i].SetString(result[i].second);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// PLACEHOLDER_ZSET_HANDLERS_6

// Helper: parse lex range bound (e.g., "-", "+", "[a", "(a")
static bool parse_lex_bound(const std::string& s, std::string* val, bool* inclusive, bool* is_min_inf,
                            bool* is_max_inf) {
	*inclusive = false;
	*is_min_inf = false;
	*is_max_inf = false;
	if (s == "-") {
		*is_min_inf = true;
		return true;
	}
	if (s == "+") {
		*is_max_inf = true;
		return true;
	}
	if (s.size() < 2)
		return false;
	if (s[0] == '[') {
		*inclusive = true;
		*val = s.substr(1);
		return true;
	}
	if (s[0] == '(') {
		*inclusive = false;
		*val = s.substr(1);
		return true;
	}
	return false;
}

class ZRangeByLexCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 4) {
			output->SetError("ERR wrong number of arguments for 'zrangebylex' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		std::string min_str(args[2].data(), args[2].size()), max_str(args[3].data(), args[3].size());
		std::string min_val, max_val;
		bool min_inc, max_inc, min_inf, max_inf;
		if (!parse_lex_bound(min_str, &min_val, &min_inc, &min_inf, &max_inf) ||
		    !parse_lex_bound(max_str, &max_val, &max_inc, &min_inf, &max_inf)) {
			output->SetError("ERR min or max not valid string range item");
			return brpc::REDIS_CMD_HANDLED;
		}
		// Re-parse properly
		bool min_is_min_inf = false, min_is_max_inf = false, max_is_min_inf = false, max_is_max_inf = false;
		parse_lex_bound(min_str, &min_val, &min_inc, &min_is_min_inf, &min_is_max_inf);
		parse_lex_bound(max_str, &max_val, &max_inc, &max_is_min_inf, &max_is_max_inf);
		int64_t limit_offset = 0, limit_count = -1;
		for (size_t i = 4; i + 2 < args.size(); i += 3) {
			std::string opt(args[i].data(), args[i].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
			if (opt == "LIMIT") {
				try {
					limit_offset = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
					limit_count = std::stoll(std::string(args[i + 2].data(), args[i + 2].size()));
				} catch (...) {
				}
			}
		}
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		std::vector<std::pair<double, std::string>> all_entries;
		load_zset_score_ordered(route.region_id, get_index_id(route), route.slot, user_key, meta.version, &all_entries);
		std::vector<std::string> result;
		for (auto& e : all_entries) {
			const std::string& m = e.second;
			if (!min_is_min_inf) {
				int cmp = m.compare(min_val);
				if (min_inc ? cmp < 0 : cmp <= 0)
					continue;
			}
			if (!max_is_max_inf) {
				int cmp = m.compare(max_val);
				if (max_inc ? cmp > 0 : cmp >= 0)
					continue;
			}
			result.push_back(m);
		}
		if (limit_count >= 0) {
			if (limit_offset >= static_cast<int64_t>(result.size()))
				result.clear();
			else
				result = std::vector<std::string>(
				    result.begin() + limit_offset,
				    (limit_count > 0 && limit_offset + limit_count < static_cast<int64_t>(result.size()))
				        ? result.begin() + limit_offset + limit_count
				        : result.end());
		}
		output->SetArray(result.size());
		for (size_t i = 0; i < result.size(); ++i)
			(*output)[i].SetString(result[i]);
		return brpc::REDIS_CMD_HANDLED;
	}
};

class ZLexCountCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'zlexcount' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		std::string min_str(args[2].data(), args[2].size()), max_str(args[3].data(), args[3].size());
		std::string min_val, max_val;
		bool min_inc, max_inc, min_inf, max_inf;
		bool min_is_min_inf = false, min_is_max_inf = false, max_is_min_inf = false, max_is_max_inf = false;
		if (!parse_lex_bound(min_str, &min_val, &min_inc, &min_is_min_inf, &min_is_max_inf)) {
			output->SetError("ERR min or max not valid string range item");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (!parse_lex_bound(max_str, &max_val, &max_inc, &max_is_min_inf, &max_is_max_inf)) {
			output->SetError("ERR min or max not valid string range item");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		std::vector<std::pair<double, std::string>> all_entries;
		load_zset_score_ordered(route.region_id, get_index_id(route), route.slot, user_key, meta.version, &all_entries);
		int64_t count = 0;
		for (auto& e : all_entries) {
			const std::string& m = e.second;
			if (!min_is_min_inf) {
				int cmp = m.compare(min_val);
				if (min_inc ? cmp < 0 : cmp <= 0)
					continue;
			}
			if (!max_is_max_inf) {
				int cmp = m.compare(max_val);
				if (max_inc ? cmp > 0 : cmp >= 0)
					continue;
			}
			++count;
		}
		output->SetInteger(count);
		return brpc::REDIS_CMD_HANDLED;
	}
};

class ZScanCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'zscan' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		bool has_match = false;
		std::string match_pattern;
		for (size_t i = 3; i + 1 < args.size(); i += 2) {
			std::string opt(args[i].data(), args[i].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
			if (opt == "MATCH") {
				has_match = true;
				match_pattern.assign(args[i + 1].data(), args[i + 1].size());
			}
		}
		neokv::RedisMetadata meta(neokv::kRedisNone, false);
		int ret = read_any_metadata(route.region_id, get_index_id(route), route.slot, user_key, &meta);
		if (ret != 0 || meta.type() != neokv::kRedisZSet) {
			output->SetArray(2);
			(*output)[0].SetString("0");
			(*output)[1].SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		std::vector<std::pair<double, std::string>> all_entries;
		load_zset_score_ordered(route.region_id, get_index_id(route), route.slot, user_key, meta.version, &all_entries);
		std::vector<std::pair<double, std::string>> filtered;
		for (auto& e : all_entries) {
			if (has_match && !glob_match(match_pattern.c_str(), e.second.c_str()))
				continue;
			filtered.push_back(e);
		}
		output->SetArray(2);
		(*output)[0].SetString("0");
		(*output)[1].SetArray(filtered.size() * 2);
		for (size_t i = 0; i < filtered.size(); ++i) {
			(*output)[1][i * 2].SetString(filtered[i].second);
			char buf[64];
			snprintf(buf, sizeof(buf), "%.17g", filtered[i].first);
			(*output)[1][i * 2 + 1].SetString(buf);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// PLACEHOLDER_ZSET_HANDLERS_7

class ZAddCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
		if (args.size() < 4) {
			output->SetError("ERR wrong number of arguments for 'zadd' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		// Parse flags
		size_t idx = 2;
		bool nx = false, xx = false, gt = false, lt = false, ch = false, incr_mode = false;
		while (idx < args.size()) {
			std::string opt(args[idx].data(), args[idx].size());
			std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
			if (opt == "NX") {
				nx = true;
				++idx;
			} else if (opt == "XX") {
				xx = true;
				++idx;
			} else if (opt == "GT") {
				gt = true;
				++idx;
			} else if (opt == "LT") {
				lt = true;
				++idx;
			} else if (opt == "CH") {
				ch = true;
				++idx;
			} else if (opt == "INCR") {
				incr_mode = true;
				++idx;
			} else
				break;
		}
		if ((args.size() - idx) % 2 != 0 || args.size() - idx < 2) {
			output->SetError("ERR syntax error");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (incr_mode && (args.size() - idx) != 2) {
			output->SetError("ERR INCR option supports a single increment-element pair");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::pb::RedisWriteRequest redis_req;
		// Encode flags in kvs[0].expire_ms: bit0=NX, bit1=XX, bit2=GT, bit3=LT, bit4=CH, bit5=INCR
		int64_t flags =
		    (nx ? 1 : 0) | (xx ? 2 : 0) | (gt ? 4 : 0) | (lt ? 8 : 0) | (ch ? 16 : 0) | (incr_mode ? 32 : 0);
		for (size_t i = idx; i + 1 < args.size(); i += 2) {
			std::string score_str(args[i].data(), args[i].size());
			std::string member(args[i + 1].data(), args[i + 1].size());
			double score;
			try {
				score = std::stod(score_str);
			} catch (...) {
				output->SetError("ERR value is not a valid float");
				return brpc::REDIS_CMD_HANDLED;
			}
			auto* entry = redis_req.add_zset_entries();
			entry->set_score(score);
			entry->set_member(member);
		}
		auto* kv_pb = redis_req.mutable_kvs()->Add();
		kv_pb->set_key(user_key);
		kv_pb->set_expire_ms(flags);
		redis_req.set_cmd(neokv::pb::REDIS_ZADD);
		redis_req.set_slot(route.slot);
		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);
		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER)
				set_moved_error(route.slot, response.leader(), output);
			else
				output->SetError(response.errmsg().c_str());
			return brpc::REDIS_CMD_HANDLED;
		}
		if (incr_mode) {
			if (response.affected_rows() == 0 && response.errmsg().empty())
				output->SetNullString();
			else {
				output->SetString(response.errmsg());
			}
		} else {
			output->SetInteger(response.affected_rows());
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class ZRemCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'zrem' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		neokv::pb::RedisWriteRequest redis_req;
		for (size_t i = 2; i < args.size(); ++i)
			redis_req.add_members(std::string(args[i].data(), args[i].size()));
		int64_t affected = 0;
		int ret = write_zset_through_raft(this, route, neokv::pb::REDIS_ZREM, user_key, output, &redis_req, &affected);
		if (ret == 0)
			output->SetInteger(affected);
		return brpc::REDIS_CMD_HANDLED;
	}
};

class ZIncrByCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'zincrby' command");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		std::string incr_str(args[2].data(), args[2].size());
		std::string member(args[3].data(), args[3].size());
		double increment;
		try {
			increment = std::stod(incr_str);
		} catch (...) {
			output->SetError("ERR value is not a valid float");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::pb::RedisWriteRequest redis_req;
		auto* entry = redis_req.add_zset_entries();
		entry->set_score(increment);
		entry->set_member(member);
		std::string result_str;
		int ret = write_zset_through_raft(this, route, neokv::pb::REDIS_ZINCRBY, user_key, output, &redis_req, nullptr,
		                                  &result_str);
		if (ret == 0)
			output->SetString(result_str);
		return brpc::REDIS_CMD_HANDLED;
	}
};

// PLACEHOLDER_ZSET_HANDLERS_8

class ZPopMinMaxCommandHandler final : public RedisDataCommandHandler {
public:
	bool pop_max_;
	explicit ZPopMinMaxCommandHandler(bool pop_max) : pop_max_(pop_max) {
	}
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		if (args.size() < 2 || args.size() > 3) {
			output->SetError("ERR wrong number of arguments");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		std::string user_key(args[1].data(), args[1].size());
		int64_t count = 1;
		if (args.size() == 3) {
			try {
				count = std::stoll(std::string(args[2].data(), args[2].size()));
			} catch (...) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
			if (count < 0) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
		}
		if (count == 0) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(pop_max_ ? neokv::pb::REDIS_ZPOPMAX : neokv::pb::REDIS_ZPOPMIN);
		redis_req.set_slot(route.slot);
		auto* kv_pb = redis_req.add_kvs();
		kv_pb->set_key(user_key);
		kv_pb->set_expire_ms(count);
		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}
		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);
		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER)
				set_moved_error(route.slot, response.leader(), output);
			else
				output->SetError(response.errmsg().c_str());
			return brpc::REDIS_CMD_HANDLED;
		}
		if (response.affected_rows() == 0) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}
		// Parse result from errmsg: "member1\nscore1\nmember2\nscore2\n..."
		std::string result_str = response.errmsg();
		std::vector<std::string> parts;
		size_t start = 0;
		for (size_t i = 0; i <= result_str.size(); ++i) {
			if (i == result_str.size() || result_str[i] == '\n') {
				if (i > start)
					parts.emplace_back(result_str.substr(start, i - start));
				start = i + 1;
			}
		}
		output->SetArray(parts.size());
		for (size_t i = 0; i < parts.size(); ++i)
			(*output)[i].SetString(parts[i]);
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// P0: String Numeric/Manipulation Command Handlers
// ============================================================================

class IncrCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// INCR key
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'incr' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, "1"); // increment = 1

		int64_t result = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_INCR, kvs, neokv::REDIS_NO_EXPIRE, output, &result);
		if (ret == 0) {
			output->SetInteger(result);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class DecrCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// DECR key
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'decr' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, "-1"); // increment = -1

		int64_t result = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_INCR, kvs, neokv::REDIS_NO_EXPIRE, output, &result);
		if (ret == 0) {
			output->SetInteger(result);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class IncrByCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// INCRBY key increment
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'incrby' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string incr_str(args[2].data(), args[2].size());
		try {
			std::stoll(incr_str);
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, incr_str);

		int64_t result = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_INCR, kvs, neokv::REDIS_NO_EXPIRE, output, &result);
		if (ret == 0) {
			output->SetInteger(result);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class DecrByCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// DECRBY key decrement
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'decrby' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string decr_str(args[2].data(), args[2].size());
		int64_t decrement = 0;
		try {
			decrement = std::stoll(decr_str);
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		// Negate the decrement to make it an increment
		std::string incr_str = std::to_string(-decrement);
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, incr_str);

		int64_t result = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_INCR, kvs, neokv::REDIS_NO_EXPIRE, output, &result);
		if (ret == 0) {
			output->SetInteger(result);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class IncrByFloatCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// INCRBYFLOAT key increment
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'incrbyfloat' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string incr_str(args[2].data(), args[2].size());
		try {
			std::stod(incr_str);
		} catch (...) {
			output->SetError("ERR value is not a valid float");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, incr_str);

		int64_t affected = 0;
		std::string result_str;
		int ret = write_through_raft_with_result(route, neokv::pb::REDIS_INCRBYFLOAT_STR, kvs, neokv::REDIS_NO_EXPIRE,
		                                         output, &affected, &result_str);
		if (ret == 0) {
			output->SetString(result_str);
		}
		return brpc::REDIS_CMD_HANDLED;
	}

private:
	int write_through_raft_with_result(const neokv::RedisRouteResult& route, neokv::pb::RedisCmd cmd,
	                                   const std::vector<std::pair<std::string, std::string>>& kvs, int64_t expire_ms,
	                                   brpc::RedisReply* output, int64_t* affected_count, std::string* result_str) {
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

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return -1;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);

		if (ret != 0) {
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
		if (result_str && response.has_errmsg() && !response.errmsg().empty()) {
			*result_str = response.errmsg();
		}
		return 0;
	}
};

class AppendCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// APPEND key value
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'append' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string value(args[2].data(), args[2].size());
		std::vector<std::pair<std::string, std::string>> kvs;
		kvs.emplace_back(user_key, value);

		int64_t new_length = 0;
		int ret = write_through_raft(route, neokv::pb::REDIS_APPEND, kvs, neokv::REDIS_NO_EXPIRE, output, &new_length);
		if (ret == 0) {
			output->SetInteger(new_length);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class GetRangeCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// GETRANGE key start end
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'getrange' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t start = 0, end = 0;
		try {
			start = std::stoll(std::string(args[2].data(), args[2].size()));
			end = std::stoll(std::string(args[3].data(), args[3].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string value;
		int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, &value, nullptr);

		if (ret != 0) {
			output->SetString("");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t len = static_cast<int64_t>(value.size());
		// Normalize negative indices
		if (start < 0)
			start = len + start;
		if (end < 0)
			end = len + end;
		if (start < 0)
			start = 0;
		if (end < 0)
			end = 0;
		if (start > len - 1 || start > end) {
			output->SetString("");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (end >= len)
			end = len - 1;

		output->SetString(value.substr(static_cast<size_t>(start), static_cast<size_t>(end - start + 1)));
		return brpc::REDIS_CMD_HANDLED;
	}
};

class SetRangeCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// SETRANGE key offset value
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'setrange' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t offset = 0;
		try {
			offset = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (offset < 0) {
			output->SetError("ERR offset is out of range");
			return brpc::REDIS_CMD_HANDLED;
		}
		// Redis limits string to 512MB
		if (offset + static_cast<int64_t>(args[3].size()) > 512 * 1024 * 1024) {
			output->SetError("ERR string exceeds maximum allowed size (512MB)");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string value(args[3].data(), args[3].size());

		// Pass offset via expire_ms (repurposed)
		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(neokv::pb::REDIS_SETRANGE);
		redis_req.set_slot(route.slot);
		auto* kv_pb = redis_req.add_kvs();
		kv_pb->set_key(user_key);
		kv_pb->set_value(value);
		kv_pb->set_expire_ms(offset); // repurpose expire_ms for offset

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);
		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER) {
				set_moved_error(route.slot, response.leader(), output);
			} else {
				output->SetError(response.errmsg().c_str());
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		output->SetInteger(response.affected_rows()); // new length
		return brpc::REDIS_CMD_HANDLED;
	}
};

class GetSetCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// GETSET key value
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'getset' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string value(args[2].data(), args[2].size());

		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(neokv::pb::REDIS_GETSET);
		redis_req.set_slot(route.slot);
		auto* kv_pb = redis_req.add_kvs();
		kv_pb->set_key(user_key);
		kv_pb->set_value(value);

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);
		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER) {
				set_moved_error(route.slot, response.leader(), output);
			} else {
				output->SetError(response.errmsg().c_str());
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		if (response.affected_rows() == 0) {
			// Old value was nil
			output->SetNullString();
		} else {
			output->SetString(response.errmsg());
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class GetDelCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// GETDEL key
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'getdel' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(neokv::pb::REDIS_GETDEL);
		redis_req.set_slot(route.slot);
		auto* kv_pb = redis_req.add_kvs();
		kv_pb->set_key(user_key);

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);
		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER) {
				set_moved_error(route.slot, response.leader(), output);
			} else {
				output->SetError(response.errmsg().c_str());
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		if (response.affected_rows() == 0) {
			output->SetNullString();
		} else {
			output->SetString(response.errmsg());
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class GetExCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// GETEX key [EX seconds | PX milliseconds | EXAT timestamp | PXAT ms-timestamp | PERSIST]
		if (args.size() < 2) {
			output->SetError("ERR wrong number of arguments for 'getex' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t expire_ms = neokv::REDIS_NO_EXPIRE;
		bool has_expire_option = false;

		for (size_t i = 2; i < args.size(); ++i) {
			if (equals_ignore_case(args[i], "ex")) {
				if (i + 1 >= args.size() || has_expire_option) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				int64_t seconds = 0;
				try {
					seconds = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (seconds <= 0) {
					output->SetError("ERR invalid expire time in 'getex' command");
					return brpc::REDIS_CMD_HANDLED;
				}
				expire_ms = neokv::RedisCodec::current_time_ms() + seconds * 1000;
				has_expire_option = true;
				++i;
			} else if (equals_ignore_case(args[i], "px")) {
				if (i + 1 >= args.size() || has_expire_option) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				int64_t ms = 0;
				try {
					ms = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (ms <= 0) {
					output->SetError("ERR invalid expire time in 'getex' command");
					return brpc::REDIS_CMD_HANDLED;
				}
				expire_ms = neokv::RedisCodec::current_time_ms() + ms;
				has_expire_option = true;
				++i;
			} else if (equals_ignore_case(args[i], "exat")) {
				if (i + 1 >= args.size() || has_expire_option) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				int64_t timestamp = 0;
				try {
					timestamp = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (timestamp <= 0) {
					output->SetError("ERR invalid expire time in 'getex' command");
					return brpc::REDIS_CMD_HANDLED;
				}
				expire_ms = timestamp * 1000;
				has_expire_option = true;
				++i;
			} else if (equals_ignore_case(args[i], "pxat")) {
				if (i + 1 >= args.size() || has_expire_option) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				int64_t ms_timestamp = 0;
				try {
					ms_timestamp = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (ms_timestamp <= 0) {
					output->SetError("ERR invalid expire time in 'getex' command");
					return brpc::REDIS_CMD_HANDLED;
				}
				expire_ms = ms_timestamp;
				has_expire_option = true;
				++i;
			} else if (equals_ignore_case(args[i], "persist")) {
				if (has_expire_option) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				expire_ms = neokv::REDIS_NO_EXPIRE;
				has_expire_option = true;
			} else {
				output->SetError("ERR syntax error");
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		if (!has_expire_option) {
			// No expire option — just GET (read-only)
			if (!check_read_consistency(route, output))
				return brpc::REDIS_CMD_HANDLED;

			std::string value;
			int ret = read_key(route.region_id, get_index_id(route), route.slot, user_key, &value, nullptr);
			if (ret == 0) {
				output->SetString(value);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		// Has expire option — need write through Raft
		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(neokv::pb::REDIS_GETEX);
		redis_req.set_slot(route.slot);
		auto* kv_pb = redis_req.add_kvs();
		kv_pb->set_key(user_key);
		kv_pb->set_expire_ms(expire_ms);

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);
		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER) {
				set_moved_error(route.slot, response.leader(), output);
			} else {
				output->SetError(response.errmsg().c_str());
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		if (response.affected_rows() == 0) {
			output->SetNullString();
		} else {
			output->SetString(response.errmsg());
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// List Command Helpers
// ============================================================================

static int read_list_metadata_full(int64_t region_id, int64_t index_id, uint16_t slot, const std::string& user_key,
                                   neokv::RedisListMetadata* list_meta) {
	auto* rocks = neokv::RocksWrapper::get_instance();
	if (rocks == nullptr)
		return -1;

	rocksdb::ReadOptions read_options;
	std::string meta_key = neokv::RedisMetadataKey::encode(region_id, index_id, slot, user_key);
	std::string raw_value;
	auto status = rocks->get(read_options, rocks->get_redis_metadata_handle(), meta_key, &raw_value);

	if (status.IsNotFound())
		return 1;
	if (!status.ok())
		return -1;

	neokv::RedisListMetadata m;
	if (!m.decode(raw_value))
		return -1;
	if (m.is_dead())
		return 1;
	if (m.type() != neokv::kRedisList)
		return 1;
	*list_meta = m;
	return 0;
}

static std::string encode_list_index_str(uint64_t index) {
	uint64_t be = neokv::KeyEncoder::host_to_be64(index);
	return std::string(reinterpret_cast<const char*>(&be), 8);
}

static int write_list_through_raft(RedisDataCommandHandler* handler, const neokv::RedisRouteResult& route,
                                   neokv::pb::RedisCmd cmd, const std::string& user_key,
                                   const std::vector<std::string>& elements,
                                   const std::vector<std::pair<std::string, std::string>>& fields, int64_t extra_param,
                                   brpc::RedisReply* output, int64_t* affected_count = nullptr,
                                   std::string* result_str = nullptr) {
	neokv::pb::RedisWriteRequest redis_req;
	redis_req.set_cmd(cmd);
	redis_req.set_slot(route.slot);

	// The list key goes in kvs[0]
	auto* kv_pb = redis_req.add_kvs();
	kv_pb->set_key(user_key);
	// Pass extra param via expire_ms (repurposed)
	if (extra_param != 0) {
		kv_pb->set_expire_ms(extra_param);
	}

	// Elements go in list_elements repeated field
	for (const auto& elem : elements) {
		redis_req.add_list_elements(elem);
	}

	// Fields for commands that need them (LINSERT pivot/element, LTRIM stop, LMPOP count)
	for (const auto& fv : fields) {
		auto* field_pb = redis_req.add_fields();
		field_pb->set_field(fv.first);
		if (!fv.second.empty()) {
			field_pb->set_value(fv.second);
		}
	}

	if (route.region == nullptr) {
		output->SetError("CLUSTERDOWN Hash slot not served");
		return -1;
	}

	neokv::pb::StoreRes response;
	int ret = route.region->exec_redis_write(redis_req, &response);

	if (ret != 0) {
		if (response.errcode() == neokv::pb::NOT_LEADER) {
			handler->set_moved_error(route.slot, response.leader(), output);
		} else {
			output->SetError(response.errmsg().c_str());
		}
		return -1;
	}

	if (affected_count) {
		*affected_count = response.affected_rows();
	}
	if (result_str && response.has_errmsg() && !response.errmsg().empty()) {
		*result_str = response.errmsg();
	}
	return 0;
}

// ============================================================================
// List Read Command Handlers
// ============================================================================

class LLenCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LLEN key
		if (args.size() != 2) {
			output->SetError("ERR wrong number of arguments for 'llen' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisListMetadata list_meta;
		int ret = read_list_metadata_full(route.region_id, get_index_id(route), route.slot, user_key, &list_meta);
		if (ret != 0) {
			// Not found or wrong type — return 0
			output->SetInteger(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		output->SetInteger(static_cast<int64_t>(list_meta.size));
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LIndexCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LINDEX key index
		if (args.size() != 3) {
			output->SetError("ERR wrong number of arguments for 'lindex' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t index = 0;
		try {
			index = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisListMetadata list_meta;
		int ret = read_list_metadata_full(route.region_id, get_index_id(route), route.slot, user_key, &list_meta);
		if (ret != 0) {
			output->SetNullString();
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t size = static_cast<int64_t>(list_meta.size);
		// Normalize negative index
		if (index < 0)
			index += size;
		if (index < 0 || index >= size) {
			output->SetNullString();
			return brpc::REDIS_CMD_HANDLED;
		}

		uint64_t actual_index = list_meta.head + static_cast<uint64_t>(index);
		std::string subkey_str = encode_list_index_str(actual_index);
		std::string subkey = neokv::RedisSubkeyKey::encode(route.region_id, get_index_id(route), route.slot, user_key,
		                                                   list_meta.version, subkey_str);

		auto* rocks = neokv::RocksWrapper::get_instance();
		rocksdb::ReadOptions read_options;
		std::string value;
		auto status = rocks->get(read_options, rocks->get_data_handle(), subkey, &value);

		if (status.ok()) {
			output->SetString(value);
		} else {
			output->SetNullString();
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LRangeCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LRANGE key start stop
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'lrange' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t start = 0, stop = 0;
		try {
			start = std::stoll(std::string(args[2].data(), args[2].size()));
			stop = std::stoll(std::string(args[3].data(), args[3].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisListMetadata list_meta;
		int ret = read_list_metadata_full(route.region_id, get_index_id(route), route.slot, user_key, &list_meta);
		if (ret != 0) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t size = static_cast<int64_t>(list_meta.size);
		// Normalize negative indices
		if (start < 0)
			start += size;
		if (stop < 0)
			stop += size;
		if (start < 0)
			start = 0;
		if (stop >= size)
			stop = size - 1;
		if (start > stop || size == 0) {
			output->SetArray(0);
			return brpc::REDIS_CMD_HANDLED;
		}

		auto* rocks = neokv::RocksWrapper::get_instance();
		rocksdb::ReadOptions read_options;
		int64_t count = stop - start + 1;
		output->SetArray(static_cast<int>(count));

		for (int64_t i = 0; i < count; ++i) {
			uint64_t actual_index = list_meta.head + static_cast<uint64_t>(start + i);
			std::string subkey_str = encode_list_index_str(actual_index);
			std::string subkey = neokv::RedisSubkeyKey::encode(route.region_id, get_index_id(route), route.slot,
			                                                   user_key, list_meta.version, subkey_str);
			std::string value;
			auto status = rocks->get(read_options, rocks->get_data_handle(), subkey, &value);
			if (status.ok()) {
				(*output)[i].SetString(value);
			} else {
				(*output)[i].SetNullString();
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LPosCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'lpos' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		std::string element(args[2].data(), args[2].size());
		int64_t rank = 1;
		int64_t count = -1; // -1 means no COUNT specified
		int64_t maxlen = 0; // 0 means no limit

		for (size_t i = 3; i < args.size(); ++i) {
			if (equals_ignore_case(args[i], "rank")) {
				if (i + 1 >= args.size()) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				try {
					rank = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (rank == 0) {
					output->SetError(
					    "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... "
					    "or use negative values meaning from the last match");
					return brpc::REDIS_CMD_HANDLED;
				}
				++i;
			} else if (equals_ignore_case(args[i], "count")) {
				if (i + 1 >= args.size()) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				try {
					count = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (count < 0) {
					output->SetError("ERR COUNT can't be negative");
					return brpc::REDIS_CMD_HANDLED;
				}
				++i;
			} else if (equals_ignore_case(args[i], "maxlen")) {
				if (i + 1 >= args.size()) {
					output->SetError("ERR syntax error");
					return brpc::REDIS_CMD_HANDLED;
				}
				try {
					maxlen = std::stoll(std::string(args[i + 1].data(), args[i + 1].size()));
				} catch (...) {
					output->SetError("ERR value is not an integer or out of range");
					return brpc::REDIS_CMD_HANDLED;
				}
				if (maxlen < 0) {
					output->SetError("ERR MAXLEN can't be negative");
					return brpc::REDIS_CMD_HANDLED;
				}
				++i;
			} else {
				output->SetError("ERR syntax error");
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		bool has_count = (count >= 0);
		if (!has_count)
			count = 1; // default: find first match

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;
		if (!check_read_consistency(route, output))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		neokv::RedisListMetadata list_meta;
		int ret = read_list_metadata_full(route.region_id, get_index_id(route), route.slot, user_key, &list_meta);
		if (ret != 0) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t size = static_cast<int64_t>(list_meta.size);
		if (size == 0) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		auto* rocks = neokv::RocksWrapper::get_instance();
		rocksdb::ReadOptions read_options;

		bool reverse = (rank < 0);
		int64_t abs_rank = reverse ? -rank : rank;
		int64_t scan_limit = (maxlen > 0) ? std::min(maxlen, size) : size;

		std::vector<int64_t> positions;
		int64_t matches_skipped = 0;

		for (int64_t i = 0; i < scan_limit; ++i) {
			int64_t logical_idx = reverse ? (size - 1 - i) : i;
			uint64_t actual_index = list_meta.head + static_cast<uint64_t>(logical_idx);
			std::string subkey_str = encode_list_index_str(actual_index);
			std::string subkey = neokv::RedisSubkeyKey::encode(route.region_id, get_index_id(route), route.slot,
			                                                   user_key, list_meta.version, subkey_str);
			std::string value;
			auto status = rocks->get(read_options, rocks->get_data_handle(), subkey, &value);
			if (!status.ok())
				continue;

			if (value == element) {
				++matches_skipped;
				if (matches_skipped >= abs_rank) {
					positions.push_back(logical_idx);
					if (count > 0 && static_cast<int64_t>(positions.size()) >= count) {
						break;
					}
				}
			}
		}

		if (has_count) {
			// Return array of positions
			output->SetArray(static_cast<int>(positions.size()));
			for (size_t i = 0; i < positions.size(); ++i) {
				(*output)[i].SetInteger(positions[i]);
			}
		} else {
			// Return single position or nil
			if (positions.empty()) {
				output->SetNullString();
			} else {
				output->SetInteger(positions[0]);
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

// ============================================================================
// List Write Command Handlers
// ============================================================================

class LPushCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LPUSH key element [element ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'lpush' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::string> elements;
		for (size_t i = 2; i < args.size(); ++i) {
			elements.emplace_back(args[i].data(), args[i].size());
		}

		std::vector<std::pair<std::string, std::string>> empty_fields;
		int64_t affected = 0;
		int ret = write_list_through_raft(this, route, neokv::pb::REDIS_LPUSH, user_key, elements, empty_fields, 0,
		                                  output, &affected);
		if (ret == 0) {
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class RPushCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// RPUSH key element [element ...]
		if (args.size() < 3) {
			output->SetError("ERR wrong number of arguments for 'rpush' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::vector<std::string> elements;
		for (size_t i = 2; i < args.size(); ++i) {
			elements.emplace_back(args[i].data(), args[i].size());
		}

		std::vector<std::pair<std::string, std::string>> empty_fields;
		int64_t affected = 0;
		int ret = write_list_through_raft(this, route, neokv::pb::REDIS_RPUSH, user_key, elements, empty_fields, 0,
		                                  output, &affected);
		if (ret == 0) {
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LPopCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LPOP key [count]
		if (args.size() < 2 || args.size() > 3) {
			output->SetError("ERR wrong number of arguments for 'lpop' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		bool has_count = (args.size() == 3);
		int64_t count = 1;
		if (has_count) {
			try {
				count = std::stoll(std::string(args[2].data(), args[2].size()));
			} catch (...) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
			if (count < 0) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		if (count == 0) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		std::vector<std::string> empty_elements;
		std::vector<std::pair<std::string, std::string>> empty_fields;
		int64_t affected = 0;
		std::string result_str;
		int ret = write_list_through_raft(this, route, neokv::pb::REDIS_LPOP, user_key, empty_elements, empty_fields,
		                                  count, output, &affected, &result_str);
		if (ret == 0) {
			if (affected == 0) {
				if (has_count) {
					output->SetNullArray();
				} else {
					output->SetNullString();
				}
			} else if (!has_count) {
				output->SetString(result_str);
			} else {
				// Parse newline-separated elements
				std::vector<std::string> popped;
				size_t start = 0;
				for (size_t i = 0; i <= result_str.size(); ++i) {
					if (i == result_str.size() || result_str[i] == '\n') {
						if (i > start) {
							popped.emplace_back(result_str.substr(start, i - start));
						}
						start = i + 1;
					}
				}
				output->SetArray(static_cast<int>(popped.size()));
				for (size_t i = 0; i < popped.size(); ++i) {
					(*output)[i].SetString(popped[i]);
				}
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class RPopCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// RPOP key [count]
		if (args.size() < 2 || args.size() > 3) {
			output->SetError("ERR wrong number of arguments for 'rpop' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		bool has_count = (args.size() == 3);
		int64_t count = 1;
		if (has_count) {
			try {
				count = std::stoll(std::string(args[2].data(), args[2].size()));
			} catch (...) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
			if (count < 0) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		if (count == 0) {
			if (has_count) {
				output->SetArray(0);
			} else {
				output->SetNullString();
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		std::vector<std::string> empty_elements;
		std::vector<std::pair<std::string, std::string>> empty_fields;
		int64_t affected = 0;
		std::string result_str;
		int ret = write_list_through_raft(this, route, neokv::pb::REDIS_RPOP, user_key, empty_elements, empty_fields,
		                                  count, output, &affected, &result_str);
		if (ret == 0) {
			if (affected == 0) {
				if (has_count) {
					output->SetNullArray();
				} else {
					output->SetNullString();
				}
			} else if (!has_count) {
				output->SetString(result_str);
			} else {
				// Parse newline-separated elements
				std::vector<std::string> popped;
				size_t start = 0;
				for (size_t i = 0; i <= result_str.size(); ++i) {
					if (i == result_str.size() || result_str[i] == '\n') {
						if (i > start) {
							popped.emplace_back(result_str.substr(start, i - start));
						}
						start = i + 1;
					}
				}
				output->SetArray(static_cast<int>(popped.size()));
				for (size_t i = 0; i < popped.size(); ++i) {
					(*output)[i].SetString(popped[i]);
				}
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LSetCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LSET key index element
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'lset' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t index = 0;
		try {
			index = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string element(args[3].data(), args[3].size());

		// Build write request: index in kvs[0].expire_ms, element in kvs[0].value
		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(neokv::pb::REDIS_LSET);
		redis_req.set_slot(route.slot);
		auto* kv_pb = redis_req.add_kvs();
		kv_pb->set_key(user_key);
		kv_pb->set_value(element);
		kv_pb->set_expire_ms(index);

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);
		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER) {
				set_moved_error(route.slot, response.leader(), output);
			} else {
				output->SetError(response.errmsg().c_str());
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		output->SetStatus("OK");
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LInsertCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LINSERT key BEFORE|AFTER pivot element
		if (args.size() != 5) {
			output->SetError("ERR wrong number of arguments for 'linsert' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t direction = 0; // 0 = BEFORE, 1 = AFTER
		if (equals_ignore_case(args[2], "before")) {
			direction = 0;
		} else if (equals_ignore_case(args[2], "after")) {
			direction = 1;
		} else {
			output->SetError("ERR syntax error");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string pivot(args[3].data(), args[3].size());
		std::string element(args[4].data(), args[4].size());

		// pivot in fields[0].field, element in fields[0].value, direction in kvs[0].expire_ms
		std::vector<std::string> empty_elements;
		std::vector<std::pair<std::string, std::string>> fields;
		fields.emplace_back(pivot, element);

		int64_t affected = 0;
		int ret = write_list_through_raft(this, route, neokv::pb::REDIS_LINSERT, user_key, empty_elements, fields,
		                                  direction, output, &affected);
		if (ret == 0) {
			output->SetInteger(affected);
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LRemCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LREM key count element
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'lrem' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t count = 0;
		try {
			count = std::stoll(std::string(args[2].data(), args[2].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());
		std::string element(args[3].data(), args[3].size());

		// count in kvs[0].expire_ms, element in kvs[0].value
		neokv::pb::RedisWriteRequest redis_req;
		redis_req.set_cmd(neokv::pb::REDIS_LREM);
		redis_req.set_slot(route.slot);
		auto* kv_pb = redis_req.add_kvs();
		kv_pb->set_key(user_key);
		kv_pb->set_value(element);
		kv_pb->set_expire_ms(count);

		if (route.region == nullptr) {
			output->SetError("CLUSTERDOWN Hash slot not served");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::pb::StoreRes response;
		int ret = route.region->exec_redis_write(redis_req, &response);
		if (ret != 0) {
			if (response.errcode() == neokv::pb::NOT_LEADER) {
				set_moved_error(route.slot, response.leader(), output);
			} else {
				output->SetError(response.errmsg().c_str());
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		output->SetInteger(response.affected_rows());
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LTrimCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LTRIM key start stop
		if (args.size() != 4) {
			output->SetError("ERR wrong number of arguments for 'ltrim' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t start = 0, stop = 0;
		try {
			start = std::stoll(std::string(args[2].data(), args[2].size()));
			stop = std::stoll(std::string(args[3].data(), args[3].size()));
		} catch (...) {
			output->SetError("ERR value is not an integer or out of range");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string user_key(args[1].data(), args[1].size());

		// start in kvs[0].expire_ms, stop in fields[0].field
		std::vector<std::string> empty_elements;
		std::vector<std::pair<std::string, std::string>> fields;
		fields.emplace_back(std::to_string(stop), "");

		int64_t affected = 0;
		int ret = write_list_through_raft(this, route, neokv::pb::REDIS_LTRIM, user_key, empty_elements, fields, start,
		                                  output, &affected);
		if (ret == 0) {
			output->SetStatus("OK");
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LMoveCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
		if (args.size() != 5) {
			output->SetError("ERR wrong number of arguments for 'lmove' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t direction_flags = 0;
		// bit 0: src direction (0=LEFT, 1=RIGHT)
		if (equals_ignore_case(args[3], "left")) {
			// direction_flags bit 0 = 0
		} else if (equals_ignore_case(args[3], "right")) {
			direction_flags |= 1;
		} else {
			output->SetError("ERR syntax error");
			return brpc::REDIS_CMD_HANDLED;
		}
		// bit 1: dst direction (0=LEFT, 2=RIGHT)
		if (equals_ignore_case(args[4], "left")) {
			// direction_flags bit 1 = 0
		} else if (equals_ignore_case(args[4], "right")) {
			direction_flags |= 2;
		} else {
			output->SetError("ERR syntax error");
			return brpc::REDIS_CMD_HANDLED;
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		std::string source(args[1].data(), args[1].size());
		std::string destination(args[2].data(), args[2].size());

		// source in kvs[0].key, dest in fields[0].field, direction in kvs[0].expire_ms
		std::vector<std::string> empty_elements;
		std::vector<std::pair<std::string, std::string>> fields;
		fields.emplace_back(destination, "");

		int64_t affected = 0;
		std::string result_str;
		int ret = write_list_through_raft(this, route, neokv::pb::REDIS_LMOVE, source, empty_elements, fields,
		                                  direction_flags, output, &affected, &result_str);
		if (ret == 0) {
			if (affected == 0) {
				output->SetNullString();
			} else {
				output->SetString(result_str);
			}
		}
		return brpc::REDIS_CMD_HANDLED;
	}
};

class LMPopCommandHandler final : public RedisDataCommandHandler {
public:
	brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args, brpc::RedisReply* output,
	                                    bool /*flush_batched*/) override {
		// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
		if (args.size() < 4) {
			output->SetError("ERR wrong number of arguments for 'lmpop' command");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t numkeys = 0;
		try {
			numkeys = std::stoll(std::string(args[1].data(), args[1].size()));
		} catch (...) {
			output->SetError("ERR numkeys can't be non-positive value");
			return brpc::REDIS_CMD_HANDLED;
		}
		if (numkeys <= 0) {
			output->SetError("ERR numkeys can't be non-positive value");
			return brpc::REDIS_CMD_HANDLED;
		}

		// args: [LMPOP, numkeys, key1, ..., keyN, LEFT|RIGHT, [COUNT, count]]
		size_t direction_idx = 2 + static_cast<size_t>(numkeys);
		if (direction_idx >= args.size()) {
			output->SetError("ERR syntax error");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t direction = 0; // 0 = LEFT, 1 = RIGHT
		if (equals_ignore_case(args[direction_idx], "left")) {
			direction = 0;
		} else if (equals_ignore_case(args[direction_idx], "right")) {
			direction = 1;
		} else {
			output->SetError("ERR syntax error");
			return brpc::REDIS_CMD_HANDLED;
		}

		int64_t count = 1;
		size_t next_idx = direction_idx + 1;
		if (next_idx < args.size()) {
			if (!equals_ignore_case(args[next_idx], "count") || next_idx + 1 >= args.size()) {
				output->SetError("ERR syntax error");
				return brpc::REDIS_CMD_HANDLED;
			}
			try {
				count = std::stoll(std::string(args[next_idx + 1].data(), args[next_idx + 1].size()));
			} catch (...) {
				output->SetError("ERR value is not an integer or out of range");
				return brpc::REDIS_CMD_HANDLED;
			}
			if (count <= 0) {
				output->SetError("ERR COUNT value of LMPOP command is not a positive value");
				return brpc::REDIS_CMD_HANDLED;
			}
		}

		neokv::RedisRouteResult route;
		if (!check_route(args, output, route))
			return brpc::REDIS_CMD_HANDLED;

		// Try each key in order until one has elements
		for (int64_t k = 0; k < numkeys; ++k) {
			size_t key_idx = 2 + static_cast<size_t>(k);
			std::string user_key(args[key_idx].data(), args[key_idx].size());

			// Check if key has elements (read metadata)
			neokv::RedisListMetadata list_meta;
			int meta_ret =
			    read_list_metadata_full(route.region_id, get_index_id(route), route.slot, user_key, &list_meta);
			if (meta_ret != 0 || list_meta.size == 0) {
				continue; // Try next key
			}

			// Found a non-empty list, pop from it
			std::vector<std::string> empty_elements;
			std::vector<std::pair<std::string, std::string>> fields;
			fields.emplace_back(std::to_string(count), "");

			int64_t affected = 0;
			std::string result_str;
			int ret = write_list_through_raft(this, route, neokv::pb::REDIS_LMPOP, user_key, empty_elements, fields,
			                                  direction, output, &affected, &result_str);
			if (ret != 0) {
				return brpc::REDIS_CMD_HANDLED; // Error already set
			}

			if (affected == 0) {
				continue; // Race condition: list became empty, try next key
			}

			// Parse newline-separated elements
			std::vector<std::string> popped;
			size_t start = 0;
			for (size_t i = 0; i <= result_str.size(); ++i) {
				if (i == result_str.size() || result_str[i] == '\n') {
					if (i > start) {
						popped.emplace_back(result_str.substr(start, i - start));
					}
					start = i + 1;
				}
			}

			// Return 2-element array: [key, [elements...]]
			output->SetArray(2);
			(*output)[0].SetString(user_key);
			(*output)[1].SetArray(static_cast<int>(popped.size()));
			for (size_t i = 0; i < popped.size(); ++i) {
				(*output)[1][i].SetString(popped[i]);
			}
			return brpc::REDIS_CMD_HANDLED;
		}

		// No non-empty list found
		output->SetNullArray();
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

bool add_handler(brpc::RedisService* service, std::unique_ptr<brpc::RedisCommandHandler> handler,
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

	if (!add_handler(service, std::make_unique<FlushdbCommandHandler>(), {"flushdb", "flushall"})) {
		DB_FATAL("failed to register FLUSHDB command");
		return false;
	}
	DB_WARNING("Redis FLUSHDB/FLUSHALL command registered");

	// Phase 2: Read commands
	if (!add_handler(service, std::make_unique<ExistsCommandHandler>(), {"exists"})) {
		DB_FATAL("failed to register EXISTS command");
		return false;
	}
	if (!add_handler(service, std::make_unique<PttlCommandHandler>(), {"pttl"})) {
		DB_FATAL("failed to register PTTL command");
		return false;
	}
	if (!add_handler(service, std::make_unique<StrlenCommandHandler>(), {"strlen"})) {
		DB_FATAL("failed to register STRLEN command");
		return false;
	}
	if (!add_handler(service, std::make_unique<TypeCommandHandler>(), {"type"})) {
		DB_FATAL("failed to register TYPE command");
		return false;
	}

	// Phase 2: Write commands
	if (!add_handler(service, std::make_unique<SetnxCommandHandler>(), {"setnx"})) {
		DB_FATAL("failed to register SETNX command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SetexCommandHandler>(), {"setex"})) {
		DB_FATAL("failed to register SETEX command");
		return false;
	}
	if (!add_handler(service, std::make_unique<PsetexCommandHandler>(), {"psetex"})) {
		DB_FATAL("failed to register PSETEX command");
		return false;
	}
	if (!add_handler(service, std::make_unique<PersistCommandHandler>(), {"persist"})) {
		DB_FATAL("failed to register PERSIST command");
		return false;
	}
	if (!add_handler(service, std::make_unique<ExpireatCommandHandler>(), {"expireat"})) {
		DB_FATAL("failed to register EXPIREAT command");
		return false;
	}
	if (!add_handler(service, std::make_unique<PexpireCommandHandler>(), {"pexpire"})) {
		DB_FATAL("failed to register PEXPIRE command");
		return false;
	}
	if (!add_handler(service, std::make_unique<PexpireatCommandHandler>(), {"pexpireat"})) {
		DB_FATAL("failed to register PEXPIREAT command");
		return false;
	}
	// UNLINK is an alias for DEL
	if (!add_handler(service, std::make_unique<DelCommandHandler>(), {"unlink"})) {
		DB_FATAL("failed to register UNLINK command");
		return false;
	}
	DB_WARNING("Redis Phase 2 commands registered: "
	           "EXISTS/PTTL/STRLEN/TYPE/SETNX/SETEX/PSETEX/PERSIST/EXPIREAT/PEXPIRE/PEXPIREAT/UNLINK");

	// Phase 3: Hash commands
	if (!add_handler(service, std::make_unique<HSetCommandHandler>(), {"hset"})) {
		DB_FATAL("failed to register HSET command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HGetCommandHandler>(), {"hget"})) {
		DB_FATAL("failed to register HGET command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HDelCommandHandler>(), {"hdel"})) {
		DB_FATAL("failed to register HDEL command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HMSetCommandHandler>(), {"hmset"})) {
		DB_FATAL("failed to register HMSET command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HMGetCommandHandler>(), {"hmget"})) {
		DB_FATAL("failed to register HMGET command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HGetAllCommandHandler>(), {"hgetall"})) {
		DB_FATAL("failed to register HGETALL command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HKeysCommandHandler>(), {"hkeys"})) {
		DB_FATAL("failed to register HKEYS command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HValsCommandHandler>(), {"hvals"})) {
		DB_FATAL("failed to register HVALS command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HLenCommandHandler>(), {"hlen"})) {
		DB_FATAL("failed to register HLEN command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HExistsCommandHandler>(), {"hexists"})) {
		DB_FATAL("failed to register HEXISTS command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HSetNXCommandHandler>(), {"hsetnx"})) {
		DB_FATAL("failed to register HSETNX command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HIncrByCommandHandler>(), {"hincrby"})) {
		DB_FATAL("failed to register HINCRBY command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HIncrByFloatCommandHandler>(), {"hincrbyfloat"})) {
		DB_FATAL("failed to register HINCRBYFLOAT command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HRandFieldCommandHandler>(), {"hrandfield"})) {
		DB_FATAL("failed to register HRANDFIELD command");
		return false;
	}
	if (!add_handler(service, std::make_unique<HScanCommandHandler>(), {"hscan"})) {
		DB_FATAL("failed to register HSCAN command");
		return false;
	}
	DB_WARNING("Redis Phase 3 Hash commands registered: "
	           "HSET/HGET/HDEL/HMSET/HMGET/HGETALL/HKEYS/HVALS/HLEN/HEXISTS/HSETNX/HINCRBY/HINCRBYFLOAT"
	           "/HRANDFIELD/HSCAN");

	// Phase 4: Set commands
	if (!add_handler(service, std::make_unique<SAddCommandHandler>(), {"sadd"})) {
		DB_FATAL("failed to register SADD command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SRemCommandHandler>(), {"srem"})) {
		DB_FATAL("failed to register SREM command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SMembersCommandHandler>(), {"smembers"})) {
		DB_FATAL("failed to register SMEMBERS command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SIsMemberCommandHandler>(), {"sismember"})) {
		DB_FATAL("failed to register SISMEMBER command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SCardCommandHandler>(), {"scard"})) {
		DB_FATAL("failed to register SCARD command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SRandMemberCommandHandler>(), {"srandmember"})) {
		DB_FATAL("failed to register SRANDMEMBER command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SPopCommandHandler>(), {"spop"})) {
		DB_FATAL("failed to register SPOP command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SMisMemberCommandHandler>(), {"smismember"})) {
		DB_FATAL("failed to register SMISMEMBER command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SScanCommandHandler>(), {"sscan"})) {
		DB_FATAL("failed to register SSCAN command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SInterCommandHandler>(), {"sinter"})) {
		DB_FATAL("failed to register SINTER command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SUnionCommandHandler>(), {"sunion"})) {
		DB_FATAL("failed to register SUNION command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SDiffCommandHandler>(), {"sdiff"})) {
		DB_FATAL("failed to register SDIFF command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SInterCardCommandHandler>(), {"sintercard"})) {
		DB_FATAL("failed to register SINTERCARD command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SMoveCommandHandler>(), {"smove"})) {
		DB_FATAL("failed to register SMOVE command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SInterStoreCommandHandler>(), {"sinterstore"})) {
		DB_FATAL("failed to register SINTERSTORE command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SUnionStoreCommandHandler>(), {"sunionstore"})) {
		DB_FATAL("failed to register SUNIONSTORE command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SDiffStoreCommandHandler>(), {"sdiffstore"})) {
		DB_FATAL("failed to register SDIFFSTORE command");
		return false;
	}
	DB_WARNING("Redis Phase 4 Set commands registered: "
	           "SADD/SREM/SMEMBERS/SISMEMBER/SCARD/SRANDMEMBER/SPOP"
	           "/SMISMEMBER/SSCAN/SINTER/SUNION/SDIFF/SINTERCARD/SMOVE"
	           "/SINTERSTORE/SUNIONSTORE/SDIFFSTORE");

	// P0: String numeric/manipulation commands
	if (!add_handler(service, std::make_unique<IncrCommandHandler>(), {"incr"})) {
		DB_FATAL("failed to register INCR command");
		return false;
	}
	if (!add_handler(service, std::make_unique<DecrCommandHandler>(), {"decr"})) {
		DB_FATAL("failed to register DECR command");
		return false;
	}
	if (!add_handler(service, std::make_unique<IncrByCommandHandler>(), {"incrby"})) {
		DB_FATAL("failed to register INCRBY command");
		return false;
	}
	if (!add_handler(service, std::make_unique<DecrByCommandHandler>(), {"decrby"})) {
		DB_FATAL("failed to register DECRBY command");
		return false;
	}
	if (!add_handler(service, std::make_unique<IncrByFloatCommandHandler>(), {"incrbyfloat"})) {
		DB_FATAL("failed to register INCRBYFLOAT command");
		return false;
	}
	if (!add_handler(service, std::make_unique<AppendCommandHandler>(), {"append"})) {
		DB_FATAL("failed to register APPEND command");
		return false;
	}
	if (!add_handler(service, std::make_unique<GetRangeCommandHandler>(), {"getrange"})) {
		DB_FATAL("failed to register GETRANGE command");
		return false;
	}
	if (!add_handler(service, std::make_unique<SetRangeCommandHandler>(), {"setrange"})) {
		DB_FATAL("failed to register SETRANGE command");
		return false;
	}
	if (!add_handler(service, std::make_unique<GetSetCommandHandler>(), {"getset"})) {
		DB_FATAL("failed to register GETSET command");
		return false;
	}
	if (!add_handler(service, std::make_unique<GetDelCommandHandler>(), {"getdel"})) {
		DB_FATAL("failed to register GETDEL command");
		return false;
	}
	if (!add_handler(service, std::make_unique<GetExCommandHandler>(), {"getex"})) {
		DB_FATAL("failed to register GETEX command");
		return false;
	}
	DB_WARNING("Redis P0 String numeric commands registered: "
	           "INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT/APPEND/GETRANGE/SETRANGE/GETSET/GETDEL/GETEX");

	// Phase 5: List commands
	if (!add_handler(service, std::make_unique<LLenCommandHandler>(), {"llen"})) {
		DB_FATAL("failed to register LLEN command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LIndexCommandHandler>(), {"lindex"})) {
		DB_FATAL("failed to register LINDEX command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LRangeCommandHandler>(), {"lrange"})) {
		DB_FATAL("failed to register LRANGE command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LPosCommandHandler>(), {"lpos"})) {
		DB_FATAL("failed to register LPOS command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LPushCommandHandler>(), {"lpush"})) {
		DB_FATAL("failed to register LPUSH command");
		return false;
	}
	if (!add_handler(service, std::make_unique<RPushCommandHandler>(), {"rpush"})) {
		DB_FATAL("failed to register RPUSH command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LPopCommandHandler>(), {"lpop"})) {
		DB_FATAL("failed to register LPOP command");
		return false;
	}
	if (!add_handler(service, std::make_unique<RPopCommandHandler>(), {"rpop"})) {
		DB_FATAL("failed to register RPOP command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LSetCommandHandler>(), {"lset"})) {
		DB_FATAL("failed to register LSET command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LInsertCommandHandler>(), {"linsert"})) {
		DB_FATAL("failed to register LINSERT command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LRemCommandHandler>(), {"lrem"})) {
		DB_FATAL("failed to register LREM command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LTrimCommandHandler>(), {"ltrim"})) {
		DB_FATAL("failed to register LTRIM command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LMoveCommandHandler>(), {"lmove"})) {
		DB_FATAL("failed to register LMOVE command");
		return false;
	}
	if (!add_handler(service, std::make_unique<LMPopCommandHandler>(), {"lmpop"})) {
		DB_FATAL("failed to register LMPOP command");
		return false;
	}
	DB_WARNING("Redis Phase 5 List commands registered: "
	           "LLEN/LINDEX/LRANGE/LPOS/LPUSH/RPUSH/LPOP/RPOP/LSET/LINSERT/LREM/LTRIM/LMOVE/LMPOP");

	// Phase 6: Sorted Set commands
	if (!add_handler(service, std::make_unique<ZAddCommandHandler>(), {"zadd"})) {
		DB_FATAL("failed to register ZADD");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZRemCommandHandler>(), {"zrem"})) {
		DB_FATAL("failed to register ZREM");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZScoreCommandHandler>(), {"zscore"})) {
		DB_FATAL("failed to register ZSCORE");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZRankCommandHandler>(false), {"zrank"})) {
		DB_FATAL("failed to register ZRANK");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZRankCommandHandler>(true), {"zrevrank"})) {
		DB_FATAL("failed to register ZREVRANK");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZCardCommandHandler>(), {"zcard"})) {
		DB_FATAL("failed to register ZCARD");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZCountCommandHandler>(), {"zcount"})) {
		DB_FATAL("failed to register ZCOUNT");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZRangeCommandHandler>(), {"zrange"})) {
		DB_FATAL("failed to register ZRANGE");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZRangeByScoreCommandHandler>(false), {"zrangebyscore"})) {
		DB_FATAL("failed to register ZRANGEBYSCORE");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZRevRangeCommandHandler>(), {"zrevrange"})) {
		DB_FATAL("failed to register ZREVRANGE");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZRangeByScoreCommandHandler>(true), {"zrevrangebyscore"})) {
		DB_FATAL("failed to register ZREVRANGEBYSCORE");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZIncrByCommandHandler>(), {"zincrby"})) {
		DB_FATAL("failed to register ZINCRBY");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZRangeByLexCommandHandler>(), {"zrangebylex"})) {
		DB_FATAL("failed to register ZRANGEBYLEX");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZLexCountCommandHandler>(), {"zlexcount"})) {
		DB_FATAL("failed to register ZLEXCOUNT");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZPopMinMaxCommandHandler>(false), {"zpopmin"})) {
		DB_FATAL("failed to register ZPOPMIN");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZPopMinMaxCommandHandler>(true), {"zpopmax"})) {
		DB_FATAL("failed to register ZPOPMAX");
		return false;
	}
	if (!add_handler(service, std::make_unique<ZScanCommandHandler>(), {"zscan"})) {
		DB_FATAL("failed to register ZSCAN");
		return false;
	}
	DB_WARNING("Redis Phase 6 Sorted Set commands registered: "
	           "ZADD/ZREM/ZSCORE/ZRANK/ZREVRANK/ZCARD/ZCOUNT/ZRANGE/ZRANGEBYSCORE"
	           "/ZREVRANGE/ZREVRANGEBYSCORE/ZINCRBY/ZRANGEBYLEX/ZLEXCOUNT/ZPOPMIN/ZPOPMAX/ZSCAN");
#endif

	return true;
}

} // namespace neokv
