// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package key

import (
	"context"
	"testing"
	"time"

	"github.com/neokv/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("EXISTS single key - exists", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "hello", 0).Err())
		n, err := rdb.Exists(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	})

	t.Run("EXISTS single key - does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		n, err := rdb.Exists(ctx, "nonexistent").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	})

	t.Run("EXISTS multiple keys", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "a{t}", "1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "b{t}", "2", 0).Err())
		n, err := rdb.Exists(ctx, "a{t}", "b{t}", "c{t}").Result()
		require.NoError(t, err)
		require.Equal(t, int64(2), n)
	})

	t.Run("EXISTS counts duplicate keys", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "hello", 0).Err())
		n, err := rdb.Exists(ctx, "mykey", "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(2), n)
	})

	t.Run("EXISTS with expired key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "hello", 1*time.Second).Err())
		time.Sleep(1100 * time.Millisecond)
		n, err := rdb.Exists(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	})

	t.Run("TYPE returns string for string key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "hello", 0).Err())
		typ, err := rdb.Type(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, "string", typ)
	})

	t.Run("TYPE returns none for non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		typ, err := rdb.Type(ctx, "nonexistent").Result()
		require.NoError(t, err)
		require.Equal(t, "none", typ)
	})

	t.Run("UNLINK single key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "hello", 0).Err())
		n, err := rdb.Unlink(ctx, "mykey").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		_, err = rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})

	t.Run("UNLINK non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		n, err := rdb.Unlink(ctx, "nonexistent").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	})

	t.Run("UNLINK multiple keys", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "a{t}", "1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "b{t}", "2", 0).Err())
		require.NoError(t, rdb.Set(ctx, "c{t}", "3", 0).Err())
		n, err := rdb.Unlink(ctx, "a{t}", "b{t}", "c{t}", "d{t}").Result()
		require.NoError(t, err)
		require.Equal(t, int64(3), n)
	})
}
