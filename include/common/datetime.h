// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub datetime header for neo-redis

#pragma once

#include <cstdint>
#include <string>
#include <ctime>

namespace neokv {

// TimeUnit enum for partition management
enum TimeUnit { TIME_UNIT_YEAR = 0, TIME_UNIT_MONTH = 1, TIME_UNIT_DAY = 2, TIME_UNIT_HOUR = 3 };

// Time functions - stub for neo-redis
inline uint32_t datetime_to_timestamp(int64_t /*datetime*/) {
	return 0;
}

inline int64_t timestamp_to_datetime(uint32_t /*timestamp*/) {
	return 0;
}

inline int64_t str_to_datetime(const char* /*str*/) {
	return 0;
}

inline std::string datetime_to_str(int64_t /*datetime*/) {
	return "";
}

} // namespace neokv
