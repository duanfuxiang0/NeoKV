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

#include "redis_codec.h"
#include <gtest/gtest.h>

namespace neokv {

TEST(RedisCodecTest, EncodeDecodeKey) {
	int64_t region_id = 12345;
	int64_t index_id = 67890;
	uint16_t slot = 1234;
	std::string user_key = "test_key";
	RedisTypeTag type_tag = REDIS_STRING;

	std::string encoded = RedisCodec::encode_key(region_id, index_id, slot, user_key, type_tag);

	// Verify minimum size
	EXPECT_GE(encoded.size(), 23u);

	// Decode and verify
	int64_t dec_region_id, dec_index_id;
	uint16_t dec_slot;
	std::string dec_user_key;
	RedisTypeTag dec_type_tag;

	EXPECT_TRUE(
	    RedisCodec::decode_key(encoded, &dec_region_id, &dec_index_id, &dec_slot, &dec_user_key, &dec_type_tag));
	EXPECT_EQ(region_id, dec_region_id);
	EXPECT_EQ(index_id, dec_index_id);
	EXPECT_EQ(slot, dec_slot);
	EXPECT_EQ(user_key, dec_user_key);
	EXPECT_EQ(type_tag, dec_type_tag);
}

TEST(RedisCodecTest, EncodeDecodeValue) {
	int64_t expire_at_ms = 1234567890123;
	std::string payload = "hello world";

	std::string encoded = RedisCodec::encode_value(expire_at_ms, payload);

	// Should be 8 bytes for expire + payload
	EXPECT_EQ(8u + payload.size(), encoded.size());

	// Decode and verify
	int64_t dec_expire;
	std::string dec_payload;

	EXPECT_TRUE(RedisCodec::decode_value(encoded, &dec_expire, &dec_payload));
	EXPECT_EQ(expire_at_ms, dec_expire);
	EXPECT_EQ(payload, dec_payload);
}

TEST(RedisCodecTest, ValueNoExpire) {
	std::string payload = "no expire test";
	std::string encoded = RedisCodec::encode_value(REDIS_NO_EXPIRE, payload);

	int64_t dec_expire;
	std::string dec_payload;
	EXPECT_TRUE(RedisCodec::decode_value(encoded, &dec_expire, &dec_payload));
	EXPECT_EQ(REDIS_NO_EXPIRE, dec_expire);
	EXPECT_EQ(payload, dec_payload);

	// No expire should not be considered expired
	EXPECT_FALSE(RedisCodec::is_expired(dec_expire, RedisCodec::current_time_ms()));
}

TEST(RedisCodecTest, IsExpired) {
	int64_t now = RedisCodec::current_time_ms();

	// Past time should be expired
	EXPECT_TRUE(RedisCodec::is_expired(now - 1000, now));

	// Future time should not be expired
	EXPECT_FALSE(RedisCodec::is_expired(now + 10000, now));

	// No expire should never be expired
	EXPECT_FALSE(RedisCodec::is_expired(REDIS_NO_EXPIRE, now));
}

TEST(RedisCodecTest, ComputeTtlSeconds) {
	// No expire returns -1
	EXPECT_EQ(-1, RedisCodec::compute_ttl_seconds(REDIS_NO_EXPIRE));

	// Expired returns 0
	int64_t past = RedisCodec::current_time_ms() - 10000;
	EXPECT_EQ(0, RedisCodec::compute_ttl_seconds(past));

	// Future returns positive seconds
	int64_t future = RedisCodec::current_time_ms() + 60000; // 60 seconds
	int64_t ttl = RedisCodec::compute_ttl_seconds(future);
	EXPECT_GE(ttl, 59);
	EXPECT_LE(ttl, 60);
}

TEST(RedisCodecTest, BuildPrefix) {
	int64_t region_id = 100;
	int64_t index_id = 200;

	std::string region_prefix = RedisCodec::build_region_prefix(region_id, index_id);
	EXPECT_EQ(16u, region_prefix.size());

	uint16_t slot = 500;
	std::string slot_prefix = RedisCodec::build_slot_prefix(region_id, index_id, slot);
	EXPECT_EQ(18u, slot_prefix.size());

	// Slot prefix should start with region prefix
	EXPECT_EQ(0, slot_prefix.compare(0, 16, region_prefix));
}

} // namespace neokv

int main(int argc, char* argv[]) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
