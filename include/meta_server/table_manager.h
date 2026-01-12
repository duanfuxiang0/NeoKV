// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Neo-redis: SQL TableManager removed. This is a minimal stub for region/cluster management.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"
#include "meta_util.h"

namespace neokv {

// Minimal TableManager for neo-redis.
// It keeps no SQL schema and provides only no-op/default behaviors required by legacy meta code paths.
class TableManager {
public:
    static TableManager* get_instance() {
        static TableManager instance;
        return &instance;
    }

    // --- Generic table/partition existence (neo-redis has no SQL schema) ---
    int whether_exist_table_partition(int64_t /*table_id*/, int64_t /*partition_id*/) {
            return 0;
    }

    // --- Replica/resource defaults ---
    int get_replica_num(int64_t /*table_id*/, int64_t& replica_num) {
        replica_num = 3;
                return 0;
    }

    int get_resource_tag(int64_t /*table_id*/, std::string& resource_tag) {
        resource_tag.clear();
                return 0;
    }

    int get_table_dist_belonged(int64_t /*table_id*/, const IdcInfo& /*instance_idc*/, IdcInfo& balance_idc) {
        balance_idc = IdcInfo();
        return 0;
    }

    int get_main_logical_room(int64_t /*table_id*/, IdcInfo& idc) {
        idc = IdcInfo();
        return 0;
    }

    void get_partition_info(int64_t /*table_id*/, int64_t /*partition_id*/,
                            int64_t& partition_replica_num, std::string& partition_resource_tag) {
        // Neo-redis: no SQL partition meta; keep defaults.
        partition_replica_num = -1;
        partition_resource_tag.clear();
    }

    // --- Region mapping helpers (disabled) ---
    bool check_region_when_update(int64_t /*table_id*/,
                                  const std::map<int64_t, std::string>& /*min_start_key*/,
                                  const std::map<int64_t, std::string>& /*max_end_key*/) {
            return true;
    }

    void update_startkey_regionid_map_old_pb(
        int64_t /*table_id*/,
        const std::map<int64_t, std::map<std::string, int64_t>>& /*key_id_map*/) {}

    void update_startkey_regionid_map(
        int64_t /*table_id*/,
        const std::map<int64_t, std::string>& /*min_start_key*/,
        const std::map<int64_t, std::string>& /*max_end_key*/,
        const std::map<int64_t, std::map<std::string, int64_t>>& /*key_id_map*/) {}

    void add_region_id(int64_t /*table_id*/, int64_t /*partition_id*/, int64_t /*region_id*/) {}

    void delete_region_ids(const std::vector<int64_t>& /*table_ids*/,
                           const std::vector<int64_t>& /*partition_ids*/,
                           const std::vector<int64_t>& /*region_ids*/) {}

    int64_t get_region_count(int64_t /*table_id*/) { return 0; }

    // start_key helpers (disabled)
    int64_t get_startkey_regionid(int64_t /*table_id*/, const std::string& /*start_key*/, int64_t /*partition_id*/) {
            return -1;
        }
    int64_t get_pre_regionid(int64_t /*table_id*/, const std::string& /*start_key*/, int64_t /*partition_id*/) {
            return -1;
        }
    void add_startkey_regionid_map(const pb::RegionInfo& /*region*/) {}
    void erase_region(int64_t /*table_id*/, int64_t /*region_id*/,
                      const std::string& /*start_key*/, int64_t /*partition_id*/) {}

    // --- Split/Merge helpers (disabled, handled by RegionManager directly in neo-redis) ---
    void add_new_region(const pb::RegionInfo& /*region*/) {}
    void check_update_region(const pb::LeaderHeartBeat& /*leader_region*/,
                             const std::shared_ptr<pb::RegionInfo>& /*master_region_info*/) {}

    // --- Binlog/pk_prefix/fast_importer (disabled) ---
    void get_binlog_table_ids(std::set<int64_t>& /*binlog_table_ids*/) {}

    void get_pk_prefix_dimensions(std::unordered_map<int64_t, int32_t>& /*dims*/) {}
    bool can_do_pk_prefix_balance() { return false; }

    bool get_pk_prefix_key(int64_t /*table_id*/,
                           const pb::PeerHeartBeat& /*peer_info*/,
                           std::string& pk_prefix_key) {
        pk_prefix_key.clear();
        return false;
    }
    bool get_pk_prefix_key(int64_t /*table_id*/,
                           const pb::RegionInfo& /*region_info*/,
                           std::string& pk_prefix_key) {
        pk_prefix_key.clear();
        return false;
    }
    bool get_pk_prefix_key(int64_t /*table_id*/,
                           int32_t /*dimension*/,
                           const std::string& /*start_key*/,
                           std::string& pk_prefix_key) {
        // Neo-redis: pk_prefix load balance is disabled.
        pk_prefix_key.clear();
        return false;
    }

    void get_clusters_in_fast_importer(std::set<std::string>& /*clusters*/) {}
    bool is_cluster_in_fast_importer(const std::string& /*cluster*/) { return false; }

    void get_table_info(const std::set<int64_t>& table_ids,
                        std::unordered_map<int64_t, int64_t>& table_replica_nums,
                        std::unordered_map<int64_t, std::unordered_map<std::string, int>>& /*table_replica_dists_maps*/,
                        std::unordered_map<int64_t, std::set<std::string>>& /*table_learner_resource_tags*/) {
        for (auto table_id : table_ids) {
            table_replica_nums[table_id] = 3;
        }
    }

private:
    TableManager() = default;
};

} // namespace neokv


