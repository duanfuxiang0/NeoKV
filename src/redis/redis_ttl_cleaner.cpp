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

#include "redis_ttl_cleaner.h"
#include "redis_codec.h"
#include "redis_common.h"
#include "rocks_wrapper.h"
#include "store.h"
#include "common.h"
#include <chrono>

namespace neokv {

RedisTTLCleaner::~RedisTTLCleaner() {
    shutdown();
}

int RedisTTLCleaner::init(int interval_seconds) {
    if (_running.load()) {
        DB_WARNING("RedisTTLCleaner already running");
        return 0;
    }

    if (interval_seconds <= 0) {
        DB_WARNING("RedisTTLCleaner disabled (interval <= 0)");
        return 0;
    }

    _interval_seconds = interval_seconds;
    _shutdown_requested.store(false);
    _running.store(true);

    _cleanup_thread = std::thread(&RedisTTLCleaner::cleanup_thread_func, this);
    DB_WARNING("RedisTTLCleaner started with interval %d seconds", interval_seconds);
    return 0;
}

void RedisTTLCleaner::shutdown() {
    if (!_running.load()) {
        return;
    }

    DB_WARNING("RedisTTLCleaner shutting down...");
    _shutdown_requested.store(true);
    
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _cv.notify_all();
    }

    if (_cleanup_thread.joinable()) {
        _cleanup_thread.join();
    }

    _running.store(false);
    DB_WARNING("RedisTTLCleaner stopped. Total cleaned: %ld", _total_cleaned.load());
}

void RedisTTLCleaner::trigger_cleanup() {
    std::lock_guard<std::mutex> lock(_mutex);
    _cv.notify_all();
}

void RedisTTLCleaner::cleanup_thread_func() {
    DB_WARNING("RedisTTLCleaner thread started");

    while (!_shutdown_requested.load()) {
        // Wait for interval or shutdown signal
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _cv.wait_for(lock, std::chrono::seconds(_interval_seconds),
                         [this]() { return _shutdown_requested.load(); });
        }

        if (_shutdown_requested.load()) {
            break;
        }

        // Perform cleanup
        int64_t cleaned = do_cleanup();
        _last_cleanup_count.store(cleaned);
        _total_cleaned.fetch_add(cleaned);

        auto now = std::chrono::system_clock::now();
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
        _last_cleanup_time_ms.store(now_ms);

        if (cleaned > 0) {
            DB_WARNING("RedisTTLCleaner: cleaned %ld expired keys", cleaned);
        }
    }

    DB_WARNING("RedisTTLCleaner thread exiting");
}

int64_t RedisTTLCleaner::do_cleanup() {
    auto* rocks = RocksWrapper::get_instance();
    if (rocks == nullptr) {
        return 0;
    }

    auto* store = Store::get_instance();
    if (store == nullptr) {
        return 0;
    }

    int64_t total_deleted = 0;
    int64_t current_time_ms = RedisCodec::current_time_ms();

    // Iterate through all regions on this store
    store->traverse_region_map([&](const SmartRegion& region) {
        if (_shutdown_requested.load()) {
            return;
        }

        int64_t region_id = region->get_region_id();
        int64_t table_id = region->get_table_id();
        
        // Use fixed Redis index_id if table_id is not set or not Redis table
        if (table_id <= 0 || !is_redis_table(table_id)) {
            table_id = REDIS_INDEX_ID;
        }

        // Build prefix for this region
        std::string prefix = RedisCodec::build_region_prefix(region_id, table_id);
        
        rocksdb::ReadOptions read_options;
        read_options.fill_cache = false;  // Don't pollute block cache
        
        rocksdb::WriteBatch batch;
        int64_t batch_count = 0;
        const int64_t max_batch_size = 1000;  // Limit batch size
        
        std::unique_ptr<rocksdb::Iterator> iter(
            rocks->new_iterator(read_options, rocks->get_data_handle()));
        
        for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
            if (_shutdown_requested.load()) {
                break;
            }

            const rocksdb::Slice& key = iter->key();
            
            // Check if still in our region's prefix
            if (!key.starts_with(prefix)) {
                break;
            }

            // Decode the value to check TTL
            const rocksdb::Slice& value = iter->value();
            if (value.size() < 8) {
                continue;  // Invalid value format
            }

            // Read expire_at_ms from value (first 8 bytes, big-endian)
            int64_t expire_at_ms = 0;
            std::string dummy;
            if (!RedisCodec::decode_value(value.ToString(), &expire_at_ms, &dummy)) {
                continue;
            }

            // Check if expired
            if (RedisCodec::is_expired(expire_at_ms, current_time_ms)) {
                batch.Delete(rocks->get_data_handle(), key);
                ++batch_count;
                ++total_deleted;

                // Commit batch if it's getting large
                if (batch_count >= max_batch_size) {
                    rocksdb::WriteOptions write_options;
                    rocks->write(write_options, &batch);
                    batch.Clear();
                    batch_count = 0;
                }
            }
        }

        // Commit remaining batch
        if (batch_count > 0) {
            rocksdb::WriteOptions write_options;
            rocks->write(write_options, &batch);
        }
    });

    return total_deleted;
}

} // namespace neokv
