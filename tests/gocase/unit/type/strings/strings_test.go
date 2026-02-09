// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package strings

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/neokv/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SET and GET an item", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "x"
		value := "foobar"
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("SET and GET an empty item", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "x"
		value := ""
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("Very big payload in GET/SET", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "x"
		value := strings.Repeat("abcd", 1000000) // 4MB
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("Very big payload random access", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		var payload [100]string
		for i := 0; i < 100; i++ {
			payload[i] = strings.Repeat("pl-"+string(rune('A'+i%26)), 1000+i*100)
			require.NoError(t, rdb.Set(ctx, "bigpayload_"+string(rune('0'+i/10))+string(rune('0'+i%10)), payload[i], 0).Err())
		}
		for i := 0; i < 100; i++ {
			val := rdb.Get(ctx, "bigpayload_"+string(rune('0'+i/10))+string(rune('0'+i%10))).Val()
			require.Equal(t, payload[i], val)
		}
	})

	t.Run("SET 10000 numeric keys and access all them in reverse order", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		for i := 0; i < 10000; i++ {
			key := string(rune(i))
			val := string(rune(i))
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
		}
		for i := 9999; i >= 0; i-- {
			key := string(rune(i))
			val := rdb.Get(ctx, key).Val()
			require.Equal(t, string(rune(i)), val)
		}
	})

	t.Run("GET returns nil for non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		val, err := rdb.Get(ctx, "nonexistent").Result()
		require.ErrorIs(t, err, redis.Nil)
		require.Equal(t, "", val)
	})

	t.Run("SET overwrites existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val1", 0).Err())
		require.Equal(t, "val1", rdb.Get(ctx, "mykey").Val())
		require.NoError(t, rdb.Set(ctx, "mykey", "val2", 0).Err())
		require.Equal(t, "val2", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SET with EX option", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 10*time.Second).Err())
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Greater(t, ttl, 5*time.Second)
		require.LessOrEqual(t, ttl, 10*time.Second)
	})

	t.Run("SET with PX option", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 50000*time.Millisecond).Err())
		ttl := rdb.TTL(ctx, "mykey").Val()
		require.Greater(t, ttl, 40*time.Second)
		require.LessOrEqual(t, ttl, 50*time.Second)
	})

	t.Run("SET with NX option - key missing", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		// Use SET ... NX (NeoKV supports NX as a sub-option of SET, not SETNX command)
		res, err := rdb.Do(ctx, "SET", "mykey", "val1", "NX").Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)
		require.Equal(t, "val1", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SET with NX option - key exists", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val1", 0).Err())
		res, err := rdb.Do(ctx, "SET", "mykey", "val2", "NX").Result()
		require.ErrorIs(t, err, redis.Nil)
		_ = res
		require.Equal(t, "val1", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SET with XX option - key exists", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val1", 0).Err())
		res, err := rdb.Do(ctx, "SET", "mykey", "val2", "XX").Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)
		require.Equal(t, "val2", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SET with XX option - key missing", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		res, err := rdb.Do(ctx, "SET", "mykey", "val1", "XX").Result()
		require.ErrorIs(t, err, redis.Nil)
		_ = res
		_, err = rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})

	t.Run("DEL single key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val", 0).Err())
		n, err := rdb.Del(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		_, err = rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})

	t.Run("DEL non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		n, err := rdb.Del(ctx, "nonexistent").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	})

	t.Run("DEL multiple keys", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		// Use hash tags to ensure all keys hash to the same slot.
		require.NoError(t, rdb.Set(ctx, "a{t}", "1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "b{t}", "2", 0).Err())
		require.NoError(t, rdb.Set(ctx, "c{t}", "3", 0).Err())
		n, err := rdb.Del(ctx, "a{t}", "b{t}", "c{t}", "d{t}").Result()
		require.NoError(t, err)
		require.Equal(t, int64(3), n)
	})

	t.Run("MGET command", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		// Use hash tags to ensure all keys hash to the same slot.
		require.NoError(t, rdb.Set(ctx, "foo{t}", "BAR", 0).Err())
		require.NoError(t, rdb.Set(ctx, "bar{t}", "FOO", 0).Err())
		vals, err := rdb.MGet(ctx, "foo{t}", "bar{t}").Result()
		require.NoError(t, err)
		require.Equal(t, []interface{}{"BAR", "FOO"}, vals)
	})

	t.Run("MGET against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		// Use hash tags to ensure all keys hash to the same slot.
		require.NoError(t, rdb.Set(ctx, "foo{t}", "BAR", 0).Err())
		vals, err := rdb.MGet(ctx, "foo{t}", "baazz{t}", "bar{t}").Result()
		require.NoError(t, err)
		require.Equal(t, "BAR", vals[0])
		require.Nil(t, vals[1])
		require.Nil(t, vals[2])
	})

	t.Run("MSET base case", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.MSet(ctx, "x{t}", "10", "y{t}", "foo bar", "z{t}", "x x x x x x x\n\n\r\n").Err())
		vals, err := rdb.MGet(ctx, "x{t}", "y{t}", "z{t}").Result()
		require.NoError(t, err)
		require.Equal(t, "10", vals[0])
		require.Equal(t, "foo bar", vals[1])
		require.Equal(t, "x x x x x x x\n\n\r\n", vals[2])
	})

	t.Run("MSET wrong number of args", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		err := rdb.MSet(ctx, "x{t}", "10", "y{t}").Err()
		require.Error(t, err)
	})

	t.Run("SET and GET binary safe strings", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		value := "a]b\x00c\xffd"
		require.NoError(t, rdb.Set(ctx, "binkey", value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, "binkey").Val())
	})

	t.Run("FLUSHDB clears all keys", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "k1", "v1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "k2", "v2", 0).Err())
		require.NoError(t, rdb.FlushDB(ctx).Err())
		_, err := rdb.Get(ctx, "k1").Result()
		require.ErrorIs(t, err, redis.Nil)
		_, err = rdb.Get(ctx, "k2").Result()
		require.ErrorIs(t, err, redis.Nil)
	})

	t.Run("SETNX - key does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		ok, err := rdb.SetNX(ctx, "mykey", "hello", 0).Result()
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "hello", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SETNX - key already exists", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "val1", 0).Err())
		ok, err := rdb.SetNX(ctx, "mykey", "val2", 0).Result()
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, "val1", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SETEX - set with expiry in seconds", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.SetEx(ctx, "mykey", "hello", 10*time.Second).Err())
		require.Equal(t, "hello", rdb.Get(ctx, "mykey").Val())
		ttl := rdb.TTL(ctx, "mykey").Val()
		util.BetweenValues(t, ttl, 8*time.Second, 10*time.Second)
	})

	t.Run("SETEX - key expires after timeout", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.SetEx(ctx, "mykey", "hello", 2*time.Second).Err())
		require.Equal(t, "hello", rdb.Get(ctx, "mykey").Val())
		time.Sleep(2100 * time.Millisecond)
		_, err := rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})

	t.Run("SETEX - invalid expire time", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		err := rdb.Do(ctx, "SETEX", "mykey", "0", "hello").Err()
		require.Error(t, err)
	})

	t.Run("PSETEX - set with expiry in milliseconds", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Do(ctx, "PSETEX", "mykey", "10000", "hello").Err())
		require.Equal(t, "hello", rdb.Get(ctx, "mykey").Val())
		ttl := rdb.TTL(ctx, "mykey").Val()
		util.BetweenValues(t, ttl, 8*time.Second, 10*time.Second)
	})

	t.Run("PSETEX - key expires after timeout", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Do(ctx, "PSETEX", "mykey", "2000", "hello").Err())
		require.Equal(t, "hello", rdb.Get(ctx, "mykey").Val())
		time.Sleep(2100 * time.Millisecond)
		_, err := rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})

	t.Run("STRLEN - existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello world", 0).Err())
		n, err := rdb.StrLen(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(11), n)
	})

	t.Run("STRLEN - non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		n, err := rdb.StrLen(ctx, "nonexistent").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	})

	t.Run("STRLEN - empty string", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "", 0).Err())
		n, err := rdb.StrLen(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	})
}
