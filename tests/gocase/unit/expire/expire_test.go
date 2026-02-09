// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package expire

import (
	"context"
	"testing"
	"time"

	"github.com/neokv/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestExpire(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("EXPIRE - set timeouts multiple times", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "x", "foobar", 0).Err())
		require.True(t, rdb.Expire(ctx, "x", 5*time.Second).Val())
		util.BetweenValues(t, rdb.TTL(ctx, "x").Val(), 4*time.Second, 5*time.Second)
		require.True(t, rdb.Expire(ctx, "x", 10*time.Second).Val())
		util.BetweenValues(t, rdb.TTL(ctx, "x").Val().Seconds(), float64(9), float64(10))
		require.NoError(t, rdb.Expire(ctx, "x", 2*time.Second).Err())
	})

	t.Run("EXPIRE - It should be still possible to read 'x'", func(t *testing.T) {
		require.Equal(t, "foobar", rdb.Get(ctx, "x").Val())
	})

	t.Run("EXPIRE - After 3 seconds the key should no longer be here", func(t *testing.T) {
		time.Sleep(3100 * time.Millisecond)
		val, err := rdb.Get(ctx, "x").Result()
		require.ErrorIs(t, err, redis.Nil)
		require.Equal(t, "", val)
	})

	t.Run("EXPIRE returns 0 against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		ok := rdb.Expire(ctx, "nonexistent", 5*time.Second).Val()
		require.False(t, ok)
	})

	t.Run("TTL returns time to live in seconds", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 10*time.Second).Err())
		ttl := rdb.TTL(ctx, "mykey").Val()
		util.BetweenValues(t, ttl, 8*time.Second, 10*time.Second)
	})

	t.Run("TTL returns -1 if key has no expire", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Equal(t, time.Duration(-1), ttl)
	})

	t.Run("TTL returns -2 if key does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		ttl := rdb.TTL(ctx, "nonexistent").Val()
		require.Equal(t, time.Duration(-2), ttl)
	})

	t.Run("SET with EX and then EXPIRE to update TTL", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 100*time.Second).Err())
		ttl := rdb.TTL(ctx, "mykey").Val()
		util.BetweenValues(t, ttl, 98*time.Second, 100*time.Second)

		// Update TTL with EXPIRE
		require.True(t, rdb.Expire(ctx, "mykey", 10*time.Second).Val())
		ttl = rdb.TTL(ctx, "mykey").Val()
		util.BetweenValues(t, ttl, 8*time.Second, 10*time.Second)
	})

	t.Run("EXPIRE with empty string as TTL should report an error", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		err := rdb.Do(ctx, "EXPIRE", "mykey", "").Err()
		require.Error(t, err)
	})

	t.Run("SET with EX creates key with TTL", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 5*time.Second).Err())
		ttl := rdb.TTL(ctx, "mykey").Val()
		util.BetweenValues(t, ttl, 3*time.Second, 5*time.Second)
		require.Equal(t, "val", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SET with EX - key expires after timeout", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 2*time.Second).Err())
		require.Equal(t, "val", rdb.Get(ctx, "mykey").Val())
		time.Sleep(2100 * time.Millisecond)
		_, err := rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})

	t.Run("DEL removes key with TTL", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 100*time.Second).Err())
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Greater(t, ttl, time.Duration(0))
		n, err := rdb.Del(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		ttl = rdb.TTL(ctx, "mykey").Val()
		require.Equal(t, time.Duration(-2), ttl)
	})

	t.Run("PERSIST removes TTL from key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 10*time.Second).Err())
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Greater(t, ttl, time.Duration(0))
		ok, err := rdb.Persist(ctx, "mykey").Result()
		require.NoError(t, err)
		require.True(t, ok)
		ttl = rdb.TTL(ctx, "mykey").Val()
		require.Equal(t, time.Duration(-1), ttl)
		require.Equal(t, "val", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("PERSIST returns 0 for key without TTL", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		ok, err := rdb.Persist(ctx, "mykey").Result()
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("PERSIST returns 0 for non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		ok, err := rdb.Persist(ctx, "nonexistent").Result()
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("PTTL returns time to live in milliseconds", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 10*time.Second).Err())
		pttl := rdb.PTTL(ctx, "mykey").Val()
		util.BetweenValues(t, pttl, 8000*time.Millisecond, 10000*time.Millisecond)
	})

	t.Run("PTTL returns -1 if key has no expire", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		pttl := rdb.PTTL(ctx, "mykey").Val()
		require.Equal(t, time.Duration(-1), pttl)
	})

	t.Run("PTTL returns -2 if key does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		pttl := rdb.PTTL(ctx, "nonexistent").Val()
		require.Equal(t, time.Duration(-2), pttl)
	})

	t.Run("EXPIREAT sets absolute Unix timestamp expiry", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		futureTime := time.Now().Add(10 * time.Second)
		ok, err := rdb.ExpireAt(ctx, "mykey", futureTime).Result()
		require.NoError(t, err)
		require.True(t, ok)
		ttl := rdb.TTL(ctx, "mykey").Val()
		util.BetweenValues(t, ttl, 8*time.Second, 10*time.Second)
	})

	t.Run("EXPIREAT returns 0 for non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		futureTime := time.Now().Add(10 * time.Second)
		ok, err := rdb.ExpireAt(ctx, "nonexistent", futureTime).Result()
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("PEXPIRE sets TTL in milliseconds", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		ok, err := rdb.PExpire(ctx, "mykey", 10000*time.Millisecond).Result()
		require.NoError(t, err)
		require.True(t, ok)
		pttl := rdb.PTTL(ctx, "mykey").Val()
		util.BetweenValues(t, pttl, 8000*time.Millisecond, 10000*time.Millisecond)
	})

	t.Run("PEXPIRE returns 0 for non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		ok, err := rdb.PExpire(ctx, "nonexistent", 10000*time.Millisecond).Result()
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("PEXPIREAT sets absolute millisecond timestamp expiry", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		futureTime := time.Now().Add(10 * time.Second)
		ok, err := rdb.PExpireAt(ctx, "mykey", futureTime).Result()
		require.NoError(t, err)
		require.True(t, ok)
		pttl := rdb.PTTL(ctx, "mykey").Val()
		util.BetweenValues(t, pttl, 8000*time.Millisecond, 10000*time.Millisecond)
	})

	t.Run("PEXPIREAT returns 0 for non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		futureTime := time.Now().Add(10 * time.Second)
		ok, err := rdb.PExpireAt(ctx, "nonexistent", futureTime).Result()
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("PEXPIRE - key expires after timeout", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		ok, err := rdb.PExpire(ctx, "mykey", 2000*time.Millisecond).Result()
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "val", rdb.Get(ctx, "mykey").Val())
		time.Sleep(2100 * time.Millisecond)
		_, err = rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})
}
