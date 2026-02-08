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

// Comprehensive tests for Redis Raft integration
// Tests: Protocol buffer messages, routing, codec, and key encoding

#include "redis_codec.h"
#include "proto/store.interface.pb.h"
#include "proto/optype.pb.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <random>
#include <chrono>

namespace neokv {

// ============================================================================
// Protocol Buffer Message Tests
// ============================================================================

class RedisProtoTest : public ::testing::Test {
protected:
	void SetUp() override {
	}
	void TearDown() override {
	}
};

TEST_F(RedisProtoTest, OpTypeExists) {
	// Verify OP_REDIS_WRITE is properly defined
	pb::OpType op = pb::OP_REDIS_WRITE;
	EXPECT_EQ(90, static_cast<int>(op));
}

TEST_F(RedisProtoTest, RedisCmdEnum) {
	// Test all Redis command types
	EXPECT_EQ(0, static_cast<int>(pb::REDIS_SET));
	EXPECT_EQ(1, static_cast<int>(pb::REDIS_DEL));
	EXPECT_EQ(2, static_cast<int>(pb::REDIS_EXPIRE));
	EXPECT_EQ(3, static_cast<int>(pb::REDIS_EXPIREAT));
	EXPECT_EQ(4, static_cast<int>(pb::REDIS_MSET));
}

TEST_F(RedisProtoTest, RedisSetConditionEnum) {
	EXPECT_EQ(0, static_cast<int>(pb::REDIS_SET_CONDITION_NONE));
	EXPECT_EQ(1, static_cast<int>(pb::REDIS_SET_CONDITION_NX));
	EXPECT_EQ(2, static_cast<int>(pb::REDIS_SET_CONDITION_XX));
}

TEST_F(RedisProtoTest, RedisKvMessage) {
	pb::RedisKv kv;

	// Test key
	kv.set_key("test_key");
	EXPECT_EQ("test_key", kv.key());

	// Test value
	kv.set_value("test_value");
	EXPECT_EQ("test_value", kv.value());

	// Test expire_ms
	kv.set_expire_ms(1234567890123);
	EXPECT_EQ(1234567890123, kv.expire_ms());

	// Test optional fields
	pb::RedisKv kv2;
	EXPECT_FALSE(kv2.has_expire_ms());
	EXPECT_FALSE(kv2.has_value());
}

TEST_F(RedisProtoTest, RedisWriteRequestMessage) {
	pb::RedisWriteRequest req;

	// Set command
	req.set_cmd(pb::REDIS_SET);
	EXPECT_EQ(pb::REDIS_SET, req.cmd());

	// Set slot
	req.set_slot(12345);
	EXPECT_EQ(12345u, req.slot());

	req.set_set_condition(pb::REDIS_SET_CONDITION_NX);
	EXPECT_EQ(pb::REDIS_SET_CONDITION_NX, req.set_condition());

	// Add key-value pairs
	auto* kv1 = req.add_kvs();
	kv1->set_key("key1");
	kv1->set_value("value1");
	kv1->set_expire_ms(0);

	auto* kv2 = req.add_kvs();
	kv2->set_key("key2");
	kv2->set_value("value2");

	EXPECT_EQ(2, req.kvs_size());
	EXPECT_EQ("key1", req.kvs(0).key());
	EXPECT_EQ("value2", req.kvs(1).value());
}

TEST_F(RedisProtoTest, StoreReqWithRedisReq) {
	pb::StoreReq store_req;

	// Set operation type
	store_req.set_op_type(pb::OP_REDIS_WRITE);
	EXPECT_EQ(pb::OP_REDIS_WRITE, store_req.op_type());

	// Set region info
	store_req.set_region_id(1001);
	store_req.set_region_version(5);

	// Set Redis request
	auto* redis_req = store_req.mutable_redis_req();
	redis_req->set_cmd(pb::REDIS_MSET);
	redis_req->set_slot(500);
	redis_req->set_set_condition(pb::REDIS_SET_CONDITION_NONE);

	auto* kv = redis_req->add_kvs();
	kv->set_key("mkey");
	kv->set_value("mvalue");

	// Verify
	EXPECT_TRUE(store_req.has_redis_req());
	EXPECT_EQ(pb::REDIS_MSET, store_req.redis_req().cmd());
	EXPECT_EQ(500u, store_req.redis_req().slot());
	EXPECT_EQ(1, store_req.redis_req().kvs_size());
}

TEST_F(RedisProtoTest, SerializeDeserialize) {
	// Create original request
	pb::StoreReq original;
	original.set_op_type(pb::OP_REDIS_WRITE);
	original.set_region_id(9999);
	original.set_region_version(1); // Required field

	auto* redis_req = original.mutable_redis_req();
	redis_req->set_cmd(pb::REDIS_SET);
	redis_req->set_slot(1234);
	redis_req->set_set_condition(pb::REDIS_SET_CONDITION_XX);

	auto* kv = redis_req->add_kvs();
	kv->set_key("serialize_test_key");
	kv->set_value("serialize_test_value");
	kv->set_expire_ms(9876543210);

	// Serialize
	std::string serialized;
	ASSERT_TRUE(original.SerializeToString(&serialized));
	EXPECT_GT(serialized.size(), 0u);

	// Deserialize
	pb::StoreReq deserialized;
	ASSERT_TRUE(deserialized.ParseFromString(serialized));

	// Verify
	EXPECT_EQ(pb::OP_REDIS_WRITE, deserialized.op_type());
	EXPECT_EQ(9999, deserialized.region_id());
	EXPECT_TRUE(deserialized.has_redis_req());
	EXPECT_EQ(pb::REDIS_SET, deserialized.redis_req().cmd());
	EXPECT_EQ(1234u, deserialized.redis_req().slot());
	EXPECT_EQ(pb::REDIS_SET_CONDITION_XX, deserialized.redis_req().set_condition());
	EXPECT_EQ(1, deserialized.redis_req().kvs_size());
	EXPECT_EQ("serialize_test_key", deserialized.redis_req().kvs(0).key());
	EXPECT_EQ("serialize_test_value", deserialized.redis_req().kvs(0).value());
	EXPECT_EQ(9876543210, deserialized.redis_req().kvs(0).expire_ms());
}

// ============================================================================
// Redis Codec Tests (Extended)
// ============================================================================

class RedisCodecExtendedTest : public ::testing::Test {
protected:
	void SetUp() override {
	}
	void TearDown() override {
	}
};

TEST_F(RedisCodecExtendedTest, KeyOrderPreservation) {
	// Test that keys are sorted by (region_id, index_id, slot, user_key)
	int64_t region_id = 100;
	int64_t index_id = 200;

	std::string key1 = RedisCodec::encode_key(region_id, index_id, 100, "aaa", REDIS_STRING);
	std::string key2 = RedisCodec::encode_key(region_id, index_id, 100, "bbb", REDIS_STRING);
	std::string key3 = RedisCodec::encode_key(region_id, index_id, 200, "aaa", REDIS_STRING);

	// Same slot: keys should be ordered by user_key
	EXPECT_LT(key1, key2);

	// Different slots: smaller slot comes first
	EXPECT_LT(key1, key3);
	EXPECT_LT(key2, key3);
}

TEST_F(RedisCodecExtendedTest, EmptyKey) {
	int64_t region_id = 1;
	int64_t index_id = 1;
	uint16_t slot = 0;

	std::string encoded = RedisCodec::encode_key(region_id, index_id, slot, "", REDIS_STRING);

	int64_t dec_region, dec_index;
	uint16_t dec_slot;
	std::string dec_key;
	RedisTypeTag dec_type;

	EXPECT_TRUE(RedisCodec::decode_key(encoded, &dec_region, &dec_index, &dec_slot, &dec_key, &dec_type));
	EXPECT_EQ("", dec_key);
}

TEST_F(RedisCodecExtendedTest, BinaryKey) {
	// Test keys with binary data (null bytes, etc.)
	std::string binary_key = "key\x00with\x01binary\xffdata";
	binary_key.resize(20); // Ensure it contains null bytes

	std::string encoded = RedisCodec::encode_key(1, 1, 100, binary_key, REDIS_STRING);

	int64_t dec_region, dec_index;
	uint16_t dec_slot;
	std::string dec_key;
	RedisTypeTag dec_type;

	EXPECT_TRUE(RedisCodec::decode_key(encoded, &dec_region, &dec_index, &dec_slot, &dec_key, &dec_type));
	EXPECT_EQ(binary_key, dec_key);
}

TEST_F(RedisCodecExtendedTest, LargeValue) {
	// Test with 1MB value
	std::string large_value(1024 * 1024, 'X');

	std::string encoded = RedisCodec::encode_value(REDIS_NO_EXPIRE, large_value);

	int64_t dec_expire;
	std::string dec_value;

	EXPECT_TRUE(RedisCodec::decode_value(encoded, &dec_expire, &dec_value));
	EXPECT_EQ(large_value.size(), dec_value.size());
	EXPECT_EQ(large_value, dec_value);
}

TEST_F(RedisCodecExtendedTest, EmptyValue) {
	std::string encoded = RedisCodec::encode_value(REDIS_NO_EXPIRE, "");

	int64_t dec_expire;
	std::string dec_value;

	EXPECT_TRUE(RedisCodec::decode_value(encoded, &dec_expire, &dec_value));
	EXPECT_EQ("", dec_value);
}

TEST_F(RedisCodecExtendedTest, AllSlotValues) {
	// Test a sampling of slot values (0, max, and randoms)
	std::vector<uint16_t> slots = {0, 1, 100, 8191, 8192, 16383};

	for (uint16_t slot : slots) {
		std::string key = "test_" + std::to_string(slot);
		std::string encoded = RedisCodec::encode_key(1, 1, slot, key, REDIS_STRING);

		int64_t dec_region, dec_index;
		uint16_t dec_slot;
		std::string dec_key;
		RedisTypeTag dec_type;

		EXPECT_TRUE(RedisCodec::decode_key(encoded, &dec_region, &dec_index, &dec_slot, &dec_key, &dec_type));
		EXPECT_EQ(slot, dec_slot) << "Failed for slot " << slot;
	}
}

TEST_F(RedisCodecExtendedTest, ExpireTimeBoundaries) {
	// Test boundary values for expire time
	std::vector<int64_t> expire_times = {REDIS_NO_EXPIRE, 0, 1, INT64_MAX - 1, INT64_MAX};

	for (int64_t expire : expire_times) {
		std::string encoded = RedisCodec::encode_value(expire, "test");

		int64_t dec_expire;
		std::string dec_value;

		EXPECT_TRUE(RedisCodec::decode_value(encoded, &dec_expire, &dec_value));
		EXPECT_EQ(expire, dec_expire) << "Failed for expire " << expire;
	}
}

TEST_F(RedisCodecExtendedTest, TtlCalculation) {
	int64_t now = RedisCodec::current_time_ms();

	// Test TTL = 1 second
	int64_t ttl = RedisCodec::compute_ttl_seconds(now + 1000);
	EXPECT_GE(ttl, 0);
	EXPECT_LE(ttl, 1);

	// Test TTL = 1 hour
	ttl = RedisCodec::compute_ttl_seconds(now + 3600 * 1000);
	EXPECT_GE(ttl, 3599);
	EXPECT_LE(ttl, 3600);

	// Test TTL = 1 day
	ttl = RedisCodec::compute_ttl_seconds(now + 86400 * 1000);
	EXPECT_GE(ttl, 86399);
	EXPECT_LE(ttl, 86400);
}

// ============================================================================
// Slot Calculation Tests (Extended)
// ============================================================================

extern uint16_t redis_crc16(const std::string& key);
extern uint16_t redis_slot(const std::string& key);

class RedisSlotExtendedTest : public ::testing::Test {
protected:
	void SetUp() override {
	}
	void TearDown() override {
	}
};

TEST_F(RedisSlotExtendedTest, SlotRange) {
	// All slots should be in range [0, 16383]
	std::vector<std::string> keys = {"a",        "abc",
	                                 "test_key", "user:1000",
	                                 "{tag}key", "very_long_key_with_many_characters_to_test_the_slot_calculation"};

	for (const auto& key : keys) {
		uint16_t slot = redis_slot(key);
		EXPECT_GE(slot, 0u);
		EXPECT_LE(slot, 16383u) << "Key: " << key << " has invalid slot " << slot;
	}
}

TEST_F(RedisSlotExtendedTest, HashTagExtraction) {
	// Keys with same hash tag should have same slot
	std::vector<std::string> tagged_keys = {"{user1000}.profile", "{user1000}.settings", "{user1000}:data",
	                                        "{user1000}"};

	uint16_t expected_slot = redis_slot("user1000");
	for (const auto& key : tagged_keys) {
		EXPECT_EQ(expected_slot, redis_slot(key)) << "Key: " << key;
	}
}

TEST_F(RedisSlotExtendedTest, EmptyHashTag) {
	// Empty hash tag {} should use full key
	std::string key1 = "{}key";
	std::string key2 = "key";

	// Empty {} uses full key, so slots should be different
	uint16_t slot1 = redis_slot(key1);
	uint16_t slot2 = redis_slot(key2);

	// They won't necessarily be equal because {} is empty
	EXPECT_NE(slot1, slot2);
}

TEST_F(RedisSlotExtendedTest, NestedBraces) {
	// Only first {} pair matters: {a{b}c} uses "a{b"
	std::string key = "{a{b}c}d";
	uint16_t expected = redis_slot("a{b");
	EXPECT_EQ(expected, redis_slot(key));
}

TEST_F(RedisSlotExtendedTest, NoBraces) {
	// No braces should use full key
	std::string key = "simple_key_no_braces";
	uint16_t expected_crc = redis_crc16(key) & 0x3FFF;
	EXPECT_EQ(expected_crc, redis_slot(key));
}

TEST_F(RedisSlotExtendedTest, CrossslotDetection) {
	// These keys should have different slots (high probability)
	std::vector<std::string> different_keys = {"key_a", "key_b", "key_c", "user:1", "user:2"};

	std::set<uint16_t> slots;
	for (const auto& key : different_keys) {
		slots.insert(redis_slot(key));
	}

	// Most likely they have different slots (could theoretically collide)
	EXPECT_GT(slots.size(), 1u);
}

// ============================================================================
// Stress Tests (can be run manually with --gtest_filter=*Stress*)
// ============================================================================

class RedisStressTest : public ::testing::Test {
protected:
	std::mt19937 gen {std::random_device {}()};

	std::string random_string(size_t length) {
		static const char charset[] = "abcdefghijklmnopqrstuvwxyz0123456789";
		std::string result;
		result.reserve(length);
		std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);
		for (size_t i = 0; i < length; ++i) {
			result += charset[dis(gen)];
		}
		return result;
	}
};

TEST_F(RedisStressTest, ManyKeysEncodeDecode) {
	const int num_keys = 10000;

	auto start = std::chrono::high_resolution_clock::now();

	for (int i = 0; i < num_keys; ++i) {
		std::string key = "key_" + std::to_string(i);
		std::string value = random_string(100);

		// Encode
		std::string rocks_key = RedisCodec::encode_key(1, 1, i % 16384, key, REDIS_STRING);
		std::string rocks_value = RedisCodec::encode_value(REDIS_NO_EXPIRE, value);

		// Decode
		int64_t dec_region, dec_index;
		uint16_t dec_slot;
		std::string dec_key;
		RedisTypeTag dec_type;

		ASSERT_TRUE(RedisCodec::decode_key(rocks_key, &dec_region, &dec_index, &dec_slot, &dec_key, &dec_type));

		int64_t dec_expire;
		std::string dec_value;
		ASSERT_TRUE(RedisCodec::decode_value(rocks_value, &dec_expire, &dec_value));

		ASSERT_EQ(key, dec_key);
		ASSERT_EQ(value, dec_value);
	}

	auto end = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

	std::cout << "Encoded/decoded " << num_keys << " keys in " << duration.count() << "ms" << std::endl;
	std::cout << "Average: " << (duration.count() * 1000.0 / num_keys) << " us/key" << std::endl;
}

TEST_F(RedisStressTest, SlotDistribution) {
	// Use enough keys to ensure good distribution
	const int num_keys = 500000; // ~30 keys per slot on average
	std::vector<int> slot_counts(16384, 0);

	for (int i = 0; i < num_keys; ++i) {
		std::string key = random_string(20);
		uint16_t slot = redis_slot(key);
		slot_counts[slot]++;
	}

	// Calculate distribution statistics
	double expected = num_keys / 16384.0;
	double variance = 0;
	int min_count = INT_MAX, max_count = 0;
	int empty_slots = 0;

	for (int count : slot_counts) {
		variance += (count - expected) * (count - expected);
		min_count = std::min(min_count, count);
		max_count = std::max(max_count, count);
		if (count == 0)
			empty_slots++;
	}
	variance /= 16384;

	std::cout << "Slot distribution for " << num_keys << " keys:" << std::endl;
	std::cout << "  Expected per slot: " << expected << std::endl;
	std::cout << "  Min: " << min_count << ", Max: " << max_count << std::endl;
	std::cout << "  Empty slots: " << empty_slots << std::endl;
	std::cout << "  Variance: " << variance << ", StdDev: " << sqrt(variance) << std::endl;

	// With ~30 keys per slot, we expect very few empty slots
	EXPECT_LT(empty_slots, 100);        // Allow some variance
	EXPECT_LT(max_count, expected * 4); // No slot should have 4x expected
}

TEST_F(RedisStressTest, ProtobufSerialization) {
	const int num_iterations = 10000;

	auto start = std::chrono::high_resolution_clock::now();

	for (int i = 0; i < num_iterations; ++i) {
		pb::StoreReq req;
		req.set_op_type(pb::OP_REDIS_WRITE);
		req.set_region_id(i);
		req.set_region_version(1);

		auto* redis_req = req.mutable_redis_req();
		redis_req->set_cmd(pb::REDIS_SET);
		redis_req->set_slot(i % 16384);

		auto* kv = redis_req->add_kvs();
		kv->set_key("key_" + std::to_string(i));
		kv->set_value(random_string(256));
		kv->set_expire_ms(REDIS_NO_EXPIRE);

		// Serialize
		std::string serialized;
		ASSERT_TRUE(req.SerializeToString(&serialized));

		// Deserialize
		pb::StoreReq req2;
		ASSERT_TRUE(req2.ParseFromString(serialized));

		ASSERT_EQ(req.region_id(), req2.region_id());
	}

	auto end = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

	std::cout << "Serialized/deserialized " << num_iterations << " requests in " << duration.count() << "ms"
	          << std::endl;
}

// ============================================================================
// Edge Case Tests
// ============================================================================

class RedisEdgeCaseTest : public ::testing::Test {
protected:
	void SetUp() override {
	}
	void TearDown() override {
	}
};

TEST_F(RedisEdgeCaseTest, MaxRegionId) {
	int64_t max_region = INT64_MAX;
	std::string encoded = RedisCodec::encode_key(max_region, 1, 0, "key", REDIS_STRING);

	int64_t dec_region, dec_index;
	uint16_t dec_slot;
	std::string dec_key;
	RedisTypeTag dec_type;

	EXPECT_TRUE(RedisCodec::decode_key(encoded, &dec_region, &dec_index, &dec_slot, &dec_key, &dec_type));
	EXPECT_EQ(max_region, dec_region);
}

TEST_F(RedisEdgeCaseTest, MaxSlot) {
	uint16_t max_slot = 16383;
	std::string encoded = RedisCodec::encode_key(1, 1, max_slot, "key", REDIS_STRING);

	int64_t dec_region, dec_index;
	uint16_t dec_slot;
	std::string dec_key;
	RedisTypeTag dec_type;

	EXPECT_TRUE(RedisCodec::decode_key(encoded, &dec_region, &dec_index, &dec_slot, &dec_key, &dec_type));
	EXPECT_EQ(max_slot, dec_slot);
}

TEST_F(RedisEdgeCaseTest, UnicodeKey) {
	std::string unicode_key = "测试键_日本語_한국어_🎉";

	std::string encoded = RedisCodec::encode_key(1, 1, 100, unicode_key, REDIS_STRING);

	int64_t dec_region, dec_index;
	uint16_t dec_slot;
	std::string dec_key;
	RedisTypeTag dec_type;

	EXPECT_TRUE(RedisCodec::decode_key(encoded, &dec_region, &dec_index, &dec_slot, &dec_key, &dec_type));
	EXPECT_EQ(unicode_key, dec_key);
}

TEST_F(RedisEdgeCaseTest, UnicodeValue) {
	std::string unicode_value = "这是一个测试值，包含中文、日本語、한국어、和emoji🎉🚀💻";

	std::string encoded = RedisCodec::encode_value(REDIS_NO_EXPIRE, unicode_value);

	int64_t dec_expire;
	std::string dec_value;

	EXPECT_TRUE(RedisCodec::decode_value(encoded, &dec_expire, &dec_value));
	EXPECT_EQ(unicode_value, dec_value);
}

TEST_F(RedisEdgeCaseTest, VeryLongKey) {
	// Test with 64KB key (Redis max is typically 512MB)
	std::string long_key(64 * 1024, 'k');

	std::string encoded = RedisCodec::encode_key(1, 1, 100, long_key, REDIS_STRING);

	int64_t dec_region, dec_index;
	uint16_t dec_slot;
	std::string dec_key;
	RedisTypeTag dec_type;

	EXPECT_TRUE(RedisCodec::decode_key(encoded, &dec_region, &dec_index, &dec_slot, &dec_key, &dec_type));
	EXPECT_EQ(long_key, dec_key);
}

} // namespace neokv

int main(int argc, char* argv[]) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
