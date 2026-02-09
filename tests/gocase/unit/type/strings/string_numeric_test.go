// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package strings

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neokv/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestStringNumeric(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	// ============================================================================
	// INCR
	// ============================================================================

	t.Run("INCR against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.Incr(ctx, "novar").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), val)
	})

	t.Run("INCR against key created by INCR itself", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.Incr(ctx, "novar")
		val, err := rdb.Incr(ctx, "novar").Result()
		require.NoError(t, err)
		require.Equal(t, int64(2), val)
	})

	t.Run("INCR against key originally set with SET", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10", 0).Err())
		val, err := rdb.Incr(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(11), val)
	})

	t.Run("INCR over 32bit value", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "novar", "17179869184", 0).Err())
		val, err := rdb.Incr(ctx, "novar").Result()
		require.NoError(t, err)
		require.Equal(t, int64(17179869185), val)
	})

	t.Run("INCR fails against key with spaces", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "novar", "    11    ", 0).Err())
		err := rdb.Incr(ctx, "novar").Err()
		require.Error(t, err)
	})

	t.Run("INCR fails against a non-numeric string", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "novar", "bar", 0).Err())
		err := rdb.Incr(ctx, "novar").Err()
		require.Error(t, err)
	})

	// ============================================================================
	// DECR
	// ============================================================================

	t.Run("DECR against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.Decr(ctx, "novar").Result()
		require.NoError(t, err)
		require.Equal(t, int64(-1), val)
	})

	t.Run("DECR against key originally set with SET", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10", 0).Err())
		val, err := rdb.Decr(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(9), val)
	})

	t.Run("DECR fails against a non-numeric string", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "novar", "bar", 0).Err())
		err := rdb.Decr(ctx, "novar").Err()
		require.Error(t, err)
	})

	// ============================================================================
	// INCRBY
	// ============================================================================

	t.Run("INCRBY against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.IncrBy(ctx, "novar", 5).Result()
		require.NoError(t, err)
		require.Equal(t, int64(5), val)
	})

	t.Run("INCRBY against key originally set with SET", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10", 0).Err())
		val, err := rdb.IncrBy(ctx, "mykey", 5).Result()
		require.NoError(t, err)
		require.Equal(t, int64(15), val)
	})

	t.Run("INCRBY with negative increment", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10", 0).Err())
		val, err := rdb.IncrBy(ctx, "mykey", -5).Result()
		require.NoError(t, err)
		require.Equal(t, int64(5), val)
	})

	// ============================================================================
	// DECRBY
	// ============================================================================

	t.Run("DECRBY against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.DecrBy(ctx, "novar", 5).Result()
		require.NoError(t, err)
		require.Equal(t, int64(-5), val)
	})

	t.Run("DECRBY against key originally set with SET", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10", 0).Err())
		val, err := rdb.DecrBy(ctx, "mykey", 3).Result()
		require.NoError(t, err)
		require.Equal(t, int64(7), val)
	})

	// ============================================================================
	// INCRBYFLOAT
	// ============================================================================

	t.Run("INCRBYFLOAT against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.IncrByFloat(ctx, "novar", 1.5).Result()
		require.NoError(t, err)
		require.Equal(t, 1.5, val)
	})

	t.Run("INCRBYFLOAT against key originally set with SET", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10.5", 0).Err())
		val, err := rdb.IncrByFloat(ctx, "mykey", 0.1).Result()
		require.NoError(t, err)
		require.InDelta(t, 10.6, val, 0.0001)
	})

	t.Run("INCRBYFLOAT with negative increment", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10.5", 0).Err())
		val, err := rdb.IncrByFloat(ctx, "mykey", -5.0).Result()
		require.NoError(t, err)
		require.InDelta(t, 5.5, val, 0.0001)
	})

	t.Run("INCRBYFLOAT over integer value", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10", 0).Err())
		val, err := rdb.IncrByFloat(ctx, "mykey", 1.1).Result()
		require.NoError(t, err)
		require.InDelta(t, 11.1, val, 0.0001)
	})

	t.Run("INCRBYFLOAT fails against non-numeric string", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "bar", 0).Err())
		err := rdb.IncrByFloat(ctx, "mykey", 1.0).Err()
		require.Error(t, err)
	})

	// ============================================================================
	// INCR preserves TTL
	// ============================================================================

	t.Run("INCR preserves existing TTL", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "10", 30*time.Second).Err())
		_, err := rdb.Incr(ctx, "mykey").Result()
		require.NoError(t, err)
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Greater(t, ttl, 20*time.Second)
	})

	// ============================================================================
	// APPEND
	// ============================================================================

	t.Run("APPEND against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		n, err := rdb.Append(ctx, "mykey", "Hello").Result()
		require.NoError(t, err)
		require.Equal(t, int64(5), n)
		require.Equal(t, "Hello", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("APPEND against existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 0).Err())
		n, err := rdb.Append(ctx, "mykey", " World").Result()
		require.NoError(t, err)
		require.Equal(t, int64(11), n)
		require.Equal(t, "Hello World", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("APPEND multiple times", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.Append(ctx, "mykey", "a")
		rdb.Append(ctx, "mykey", "b")
		n, err := rdb.Append(ctx, "mykey", "c").Result()
		require.NoError(t, err)
		require.Equal(t, int64(3), n)
		require.Equal(t, "abc", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("APPEND preserves existing TTL", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 30*time.Second).Err())
		n, err := rdb.Append(ctx, "mykey", " World").Result()
		require.NoError(t, err)
		require.Equal(t, int64(11), n)
		require.Equal(t, "Hello World", rdb.Get(ctx, "mykey").Val())
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Greater(t, ttl, 20*time.Second)
	})

	t.Run("APPEND empty string keeps original value", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 0).Err())
		n, err := rdb.Append(ctx, "mykey", "").Result()
		require.NoError(t, err)
		require.Equal(t, int64(5), n)
		require.Equal(t, "Hello", rdb.Get(ctx, "mykey").Val())
	})

	// ============================================================================
	// GETRANGE
	// ============================================================================

	t.Run("GETRANGE basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello, World!", 0).Err())
		val, err := rdb.GetRange(ctx, "mykey", 0, 4).Result()
		require.NoError(t, err)
		require.Equal(t, "Hello", val)
	})

	t.Run("GETRANGE with negative indices", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello, World!", 0).Err())
		// -6 = index 7 ('W'), -1 = index 12 ('!')
		val, err := rdb.GetRange(ctx, "mykey", -6, -1).Result()
		require.NoError(t, err)
		require.Equal(t, "World!", val)
	})

	t.Run("GETRANGE with negative indices 2", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello, World!", 0).Err())
		// -5 = index 8 ('o'), -1 = index 12 ('!')
		val, err := rdb.GetRange(ctx, "mykey", -5, -1).Result()
		require.NoError(t, err)
		require.Equal(t, "orld!", val)
	})

	t.Run("GETRANGE entire string", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello, World!", 0).Err())
		val, err := rdb.GetRange(ctx, "mykey", 0, -1).Result()
		require.NoError(t, err)
		require.Equal(t, "Hello, World!", val)
	})

	t.Run("GETRANGE out of range", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello, World!", 0).Err())
		val, err := rdb.GetRange(ctx, "mykey", 0, 100).Result()
		require.NoError(t, err)
		require.Equal(t, "Hello, World!", val)
	})

	t.Run("GETRANGE non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.GetRange(ctx, "nonexistent", 0, 10).Result()
		require.NoError(t, err)
		require.Equal(t, "", val)
	})

	t.Run("GETRANGE start > end returns empty", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 0).Err())
		val, err := rdb.GetRange(ctx, "mykey", 3, 1).Result()
		require.NoError(t, err)
		require.Equal(t, "", val)
	})

	// ============================================================================
	// SETRANGE
	// ============================================================================

	t.Run("SETRANGE basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello World", 0).Err())
		n, err := rdb.SetRange(ctx, "mykey", 6, "Redis").Result()
		require.NoError(t, err)
		require.Equal(t, int64(11), n)
		require.Equal(t, "Hello Redis", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SETRANGE zero padding", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		n, err := rdb.SetRange(ctx, "mykey", 5, "Redis").Result()
		require.NoError(t, err)
		require.Equal(t, int64(10), n)
		val := rdb.Get(ctx, "mykey").Val()
		require.Equal(t, 10, len(val))
		// First 5 bytes should be zero bytes
		require.Equal(t, "\x00\x00\x00\x00\x00Redis", val)
	})

	t.Run("SETRANGE extends string", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 0).Err())
		n, err := rdb.SetRange(ctx, "mykey", 5, " World").Result()
		require.NoError(t, err)
		require.Equal(t, int64(11), n)
		require.Equal(t, "Hello World", rdb.Get(ctx, "mykey").Val())
	})

	// ============================================================================
	// GETSET
	// ============================================================================

	t.Run("GETSET - key exists", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 0).Err())
		val, err := rdb.GetSet(ctx, "mykey", "World").Result()
		require.NoError(t, err)
		require.Equal(t, "Hello", val)
		require.Equal(t, "World", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("GETSET - key does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.GetSet(ctx, "mykey", "World").Result()
		require.ErrorIs(t, err, redis.Nil)
		require.Equal(t, "", val)
		require.Equal(t, "World", rdb.Get(ctx, "mykey").Val())
	})

	// ============================================================================
	// GETDEL
	// ============================================================================

	t.Run("GETDEL - key exists", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 0).Err())
		val, err := rdb.GetDel(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, "Hello", val)
		// Key should be deleted
		_, err = rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})

	t.Run("GETDEL - key does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.GetDel(ctx, "nonexistent").Result()
		require.ErrorIs(t, err, redis.Nil)
		require.Equal(t, "", val)
	})

	// ============================================================================
	// GETEX
	// ============================================================================

	t.Run("GETEX with EX option", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 0).Err())
		val, err := rdb.GetEx(ctx, "mykey", 10*time.Second).Result()
		require.NoError(t, err)
		require.Equal(t, "Hello", val)
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Greater(t, ttl, 5*time.Second)
		require.LessOrEqual(t, ttl, 10*time.Second)
	})

	t.Run("GETEX with PERSIST option", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 30*time.Second).Err())
		// Verify TTL is set
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Greater(t, ttl, time.Duration(0))
		// GETEX with PERSIST removes TTL
		val, err := rdb.Do(ctx, "GETEX", "mykey", "PERSIST").Result()
		require.NoError(t, err)
		require.Equal(t, "Hello", val)
		ttl = rdb.TTL(ctx, "mykey").Val()
		require.Equal(t, time.Duration(-1), ttl) // -1 means no expire
	})

	t.Run("GETEX without options acts as GET", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello", 0).Err())
		val, err := rdb.Do(ctx, "GETEX", "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, "Hello", val)
	})

	t.Run("GETEX - key does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.Do(ctx, "GETEX", "nonexistent", "EX", "10").Result()
		require.ErrorIs(t, err, redis.Nil)
		_ = val
	})

	// ============================================================================
	// Overflow tests
	// ============================================================================

	t.Run("INCR overflow", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", strconv.FormatInt(math.MaxInt64, 10), 0).Err())
		err := rdb.Incr(ctx, "mykey").Err()
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "overflow"))
	})

	t.Run("DECR underflow", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", strconv.FormatInt(math.MinInt64, 10), 0).Err())
		err := rdb.Decr(ctx, "mykey").Err()
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "overflow"))
	})
}
