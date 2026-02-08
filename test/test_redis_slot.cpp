#include "redis_service.h"
#include <gtest/gtest.h>

namespace neokv {

TEST(RedisSlotTest, CrcMatchesRedis) {
	EXPECT_EQ(0x31C3, redis_crc16("123456789"));
	EXPECT_EQ(12739, redis_slot("123456789"));
}

TEST(RedisSlotTest, HashTagRule) {
	EXPECT_EQ(redis_slot("{user1000}.x"), redis_slot("user1000"));
	EXPECT_EQ(redis_slot("plain_key"), redis_slot("plain_key"));
}

} // namespace neokv

int main(int argc, char* argv[]) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
