// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package hash

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/neokv/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestHash(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("HSET and HGET single field", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "field1", "value1").Val())
		require.Equal(t, "value1", rdb.HGet(ctx, key, "field1").Val())
	})

	t.Run("HSET multiple fields", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		result := rdb.HSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3")
		require.NoError(t, result.Err())
		require.Equal(t, int64(3), result.Val())
		require.Equal(t, "v1", rdb.HGet(ctx, key, "f1").Val())
		require.Equal(t, "v2", rdb.HGet(ctx, key, "f2").Val())
		require.Equal(t, "v3", rdb.HGet(ctx, key, "f3").Val())
	})

	t.Run("HSET overwrites existing field", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "field1", "value1").Val())
		// Overwrite returns 0 (no new fields added)
		require.Equal(t, int64(0), rdb.HSet(ctx, key, "field1", "newvalue").Val())
		require.Equal(t, "newvalue", rdb.HGet(ctx, key, "field1").Val())
	})

	t.Run("HGET non-existing field", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "field1", "value1").Val())
		err := rdb.HGet(ctx, key, "nonexistent").Err()
		require.Equal(t, redis.Nil, err)
	})

	t.Run("HGET non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		err := rdb.HGet(ctx, "{t}nonexistent", "field1").Err()
		require.Equal(t, redis.Nil, err)
	})

	t.Run("HDEL single field", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2")
		require.Equal(t, int64(1), rdb.HDel(ctx, key, "f1").Val())
		err := rdb.HGet(ctx, key, "f1").Err()
		require.Equal(t, redis.Nil, err)
		require.Equal(t, "v2", rdb.HGet(ctx, key, "f2").Val())
	})

	t.Run("HDEL multiple fields", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3")
		require.Equal(t, int64(2), rdb.HDel(ctx, key, "f1", "f3").Val())
		require.Equal(t, int64(1), rdb.HLen(ctx, key).Val())
	})

	t.Run("HDEL non-existing field", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1")
		require.Equal(t, int64(0), rdb.HDel(ctx, key, "nonexistent").Val())
	})

	t.Run("HDEL non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.Equal(t, int64(0), rdb.HDel(ctx, "{t}nonexistent", "f1").Val())
	})

	t.Run("HMSET and HMGET", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		require.NoError(t, rdb.HMSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3").Err())
		vals := rdb.HMGet(ctx, key, "f1", "f2", "f3", "nonexistent").Val()
		require.Equal(t, 4, len(vals))
		require.Equal(t, "v1", vals[0])
		require.Equal(t, "v2", vals[1])
		require.Equal(t, "v3", vals[2])
		require.Nil(t, vals[3])
	})

	t.Run("HGETALL", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2")
		result := rdb.HGetAll(ctx, key).Val()
		require.Equal(t, 2, len(result))
		require.Equal(t, "v1", result["f1"])
		require.Equal(t, "v2", result["f2"])
	})

	t.Run("HGETALL empty hash", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.HGetAll(ctx, "{t}nonexistent").Val()
		require.Equal(t, 0, len(result))
	})

	t.Run("HKEYS", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3")
		keys := rdb.HKeys(ctx, key).Val()
		require.Equal(t, 3, len(keys))
		// Keys may be in any order, check they contain the expected values
		keySet := make(map[string]bool)
		for _, k := range keys {
			keySet[k] = true
		}
		require.True(t, keySet["f1"])
		require.True(t, keySet["f2"])
		require.True(t, keySet["f3"])
	})

	t.Run("HVALS", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3")
		vals := rdb.HVals(ctx, key).Val()
		require.Equal(t, 3, len(vals))
		valSet := make(map[string]bool)
		for _, v := range vals {
			valSet[v] = true
		}
		require.True(t, valSet["v1"])
		require.True(t, valSet["v2"])
		require.True(t, valSet["v3"])
	})

	t.Run("HLEN", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		require.Equal(t, int64(0), rdb.HLen(ctx, "{t}nonexistent").Val())
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2")
		require.Equal(t, int64(2), rdb.HLen(ctx, key).Val())
	})

	t.Run("HEXISTS", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1")
		require.True(t, rdb.HExists(ctx, key, "f1").Val())
		require.False(t, rdb.HExists(ctx, key, "nonexistent").Val())
		require.False(t, rdb.HExists(ctx, "{t}nonexistent", "f1").Val())
	})

	t.Run("HSETNX - field does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		require.True(t, rdb.HSetNX(ctx, key, "f1", "v1").Val())
		require.Equal(t, "v1", rdb.HGet(ctx, key, "f1").Val())
	})

	t.Run("HSETNX - field already exists", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1")
		require.False(t, rdb.HSetNX(ctx, key, "f1", "newvalue").Val())
		require.Equal(t, "v1", rdb.HGet(ctx, key, "f1").Val())
	})

	t.Run("HINCRBY", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		// Increment non-existing field (starts from 0)
		require.Equal(t, int64(5), rdb.HIncrBy(ctx, key, "counter", 5).Val())
		require.Equal(t, int64(15), rdb.HIncrBy(ctx, key, "counter", 10).Val())
		require.Equal(t, int64(10), rdb.HIncrBy(ctx, key, "counter", -5).Val())
	})

	t.Run("HINCRBYFLOAT", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		val, err := rdb.HIncrByFloat(ctx, key, "price", 10.5).Result()
		require.NoError(t, err)
		require.InDelta(t, 10.5, val, 0.001)
		val, err = rdb.HIncrByFloat(ctx, key, "price", 0.1).Result()
		require.NoError(t, err)
		require.InDelta(t, 10.6, val, 0.001)
	})

	t.Run("DEL removes hash key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2")
		require.Equal(t, int64(1), rdb.Del(ctx, key).Val())
		require.Equal(t, int64(0), rdb.HLen(ctx, key).Val())
		err := rdb.HGet(ctx, key, "f1").Err()
		require.Equal(t, redis.Nil, err)
	})

	t.Run("EXISTS works with hash key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1")
		require.Equal(t, int64(1), rdb.Exists(ctx, key).Val())
		rdb.Del(ctx, key)
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	})

	t.Run("TYPE returns hash for hash key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1")
		require.Equal(t, "hash", rdb.Type(ctx, key).Val())
	})

	t.Run("HSET large number of fields", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}bighash"
		for i := 0; i < 100; i++ {
			field := fmt.Sprintf("field_%d", i)
			value := fmt.Sprintf("value_%d", i)
			rdb.HSet(ctx, key, field, value)
		}
		require.Equal(t, int64(100), rdb.HLen(ctx, key).Val())
		for i := 0; i < 100; i++ {
			field := fmt.Sprintf("field_%d", i)
			expected := fmt.Sprintf("value_%d", i)
			require.Equal(t, expected, rdb.HGet(ctx, key, field).Val())
		}
	})

	t.Run("HDEL all fields removes hash", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2")
		rdb.HDel(ctx, key, "f1", "f2")
		require.Equal(t, int64(0), rdb.HLen(ctx, key).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	})

	t.Run("HINCRBY on string field returns error", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "name", "hello")
		err := rdb.HIncrBy(ctx, key, "name", 1).Err()
		// Should return an error because "hello" is not an integer
		require.Error(t, err)
	})

	t.Run("HSET with empty value", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "f1", "").Val())
		require.Equal(t, "", rdb.HGet(ctx, key, "f1").Val())
	})

	t.Run("HINCRBY creates field if not exists", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		val := rdb.HIncrBy(ctx, key, "newfield", 42).Val()
		require.Equal(t, int64(42), val)
		require.Equal(t, "42", rdb.HGet(ctx, key, "newfield").Val())
	})

	t.Run("FLUSHDB clears hash keys", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1")
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	})

	t.Run("HMGET on non-existing key returns nils", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		vals := rdb.HMGet(ctx, "{t}nonexistent", "f1", "f2").Val()
		require.Equal(t, 2, len(vals))
		require.Nil(t, vals[0])
		require.Nil(t, vals[1])
	})

	t.Run("HINCRBY with negative increment", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "counter", strconv.Itoa(100))
		require.Equal(t, int64(90), rdb.HIncrBy(ctx, key, "counter", -10).Val())
	})

	// ============================================================================
	// HRANDFIELD tests
	// ============================================================================

	t.Run("HRANDFIELD single field", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3")
		// Without count, returns a single field
		result := rdb.HRandField(ctx, key, 1)
		require.NoError(t, result.Err())
		require.Equal(t, 1, len(result.Val()))
		field := result.Val()[0]
		require.Contains(t, []string{"f1", "f2", "f3"}, field)
	})

	t.Run("HRANDFIELD positive count", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3")
		// Positive count: unique fields
		result := rdb.HRandField(ctx, key, 2)
		require.NoError(t, result.Err())
		require.Equal(t, 2, len(result.Val()))
		// All returned fields should be unique
		seen := make(map[string]bool)
		for _, f := range result.Val() {
			require.Contains(t, []string{"f1", "f2", "f3"}, f)
			require.False(t, seen[f], "duplicate field returned")
			seen[f] = true
		}
	})

	t.Run("HRANDFIELD count greater than hash size", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2")
		result := rdb.HRandField(ctx, key, 5)
		require.NoError(t, result.Err())
		require.Equal(t, 2, len(result.Val()))
	})

	t.Run("HRANDFIELD negative count allows duplicates", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1")
		// Negative count: may return duplicates
		result := rdb.HRandField(ctx, key, -5)
		require.NoError(t, result.Err())
		require.Equal(t, 5, len(result.Val()))
		// All should be "f1" since there's only one field
		for _, f := range result.Val() {
			require.Equal(t, "f1", f)
		}
	})

	t.Run("HRANDFIELD non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.HRandField(ctx, "{t}nosuchkey", 1)
		require.NoError(t, result.Err())
		require.Equal(t, 0, len(result.Val()))
	})

	t.Run("HRANDFIELD with WITHVALUES", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3")
		result := rdb.HRandFieldWithValues(ctx, key, 2)
		require.NoError(t, result.Err())
		require.Equal(t, 2, len(result.Val()))
		validPairs := map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"}
		for _, kv := range result.Val() {
			require.Contains(t, validPairs, kv.Key)
			require.Equal(t, validPairs[kv.Key], kv.Value)
		}
	})

	// ============================================================================
	// HSCAN tests
	// ============================================================================

	t.Run("HSCAN basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "f1", "v1", "f2", "v2", "f3", "v3")
		keys, cursor, err := rdb.HScan(ctx, key, 0, "*", 100).Result()
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor) // scan complete
		// keys contains [field, value, field, value, ...]
		require.Equal(t, 6, len(keys))
	})

	t.Run("HSCAN with MATCH pattern", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myhash"
		rdb.HSet(ctx, key, "name", "alice", "age", "30", "nickname", "ali")
		keys, cursor, err := rdb.HScan(ctx, key, 0, "n*", 100).Result()
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
		// Should match "name" and "nickname"
		require.Equal(t, 4, len(keys)) // 2 fields * 2 (field+value)
	})

	t.Run("HSCAN non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		keys, cursor, err := rdb.HScan(ctx, "{t}nosuchkey", 0, "*", 100).Result()
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
		require.Equal(t, 0, len(keys))
	})
}
