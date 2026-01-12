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

#pragma once

#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace neokv {

// Background cleaner for expired Redis keys
// Periodically scans and deletes keys that have exceeded their TTL
class RedisTTLCleaner {
public:
    static RedisTTLCleaner* get_instance() {
        static RedisTTLCleaner instance;
        return &instance;
    }

    // Initialize and start the cleaner thread
    // interval_seconds: how often to run cleanup (0 = disabled)
    int init(int interval_seconds = 60);

    // Stop the cleaner thread
    void shutdown();

    // Manually trigger a cleanup cycle (for testing)
    void trigger_cleanup();

    // Get statistics
    int64_t get_total_cleaned() const { return _total_cleaned.load(); }
    int64_t get_last_cleanup_count() const { return _last_cleanup_count.load(); }
    int64_t get_last_cleanup_time_ms() const { return _last_cleanup_time_ms.load(); }

private:
    RedisTTLCleaner() = default;
    ~RedisTTLCleaner();
    RedisTTLCleaner(const RedisTTLCleaner&) = delete;
    RedisTTLCleaner& operator=(const RedisTTLCleaner&) = delete;

    void cleanup_thread_func();
    int64_t do_cleanup();

    std::atomic<bool> _running{false};
    std::atomic<bool> _shutdown_requested{false};
    int _interval_seconds{60};
    
    std::thread _cleanup_thread;
    std::mutex _mutex;
    std::condition_variable _cv;

    std::atomic<int64_t> _total_cleaned{0};
    std::atomic<int64_t> _last_cleanup_count{0};
    std::atomic<int64_t> _last_cleanup_time_ms{0};
};

} // namespace neokv
