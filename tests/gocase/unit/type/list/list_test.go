// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package list

import (
	"context"
	"testing"

	"github.com/neokv/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("LPUSH single element", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		result := rdb.LPush(ctx, key, "hello")
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val())
	})

	t.Run("LPUSH multiple elements", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		result := rdb.LPush(ctx, key, "a", "b", "c")
		require.NoError(t, result.Err())
		require.Equal(t, int64(3), result.Val())
	})

	t.Run("RPUSH single element", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		result := rdb.RPush(ctx, key, "hello")
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val())
	})

	t.Run("RPUSH multiple elements", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		result := rdb.RPush(ctx, key, "a", "b", "c")
		require.NoError(t, result.Err())
		require.Equal(t, int64(3), result.Val())
	})

	t.Run("LPUSH ordering", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.LPush(ctx, key, "a", "b", "c")
		result := rdb.LRange(ctx, key, 0, -1)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"c", "b", "a"}, result.Val())
	})

	t.Run("RPUSH ordering", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LRange(ctx, key, 0, -1)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"a", "b", "c"}, result.Val())
	})

	t.Run("LPUSH and RPUSH combined", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b")
		rdb.LPush(ctx, key, "c", "d")
		result := rdb.LRange(ctx, key, 0, -1)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"d", "c", "a", "b"}, result.Val())
	})

	t.Run("LLEN after pushes", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		require.Equal(t, int64(0), rdb.LLen(ctx, key).Val())
		rdb.RPush(ctx, key, "a", "b", "c")
		require.Equal(t, int64(3), rdb.LLen(ctx, key).Val())
		rdb.LPush(ctx, key, "d", "e")
		require.Equal(t, int64(5), rdb.LLen(ctx, key).Val())
	})

	t.Run("LLEN non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.Equal(t, int64(0), rdb.LLen(ctx, "{t}nosuchkey").Val())
	})

	t.Run("LPOP basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LPop(ctx, key)
		require.NoError(t, result.Err())
		require.Equal(t, "a", result.Val())
		require.Equal(t, int64(2), rdb.LLen(ctx, key).Val())
	})

	t.Run("RPOP basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.RPop(ctx, key)
		require.NoError(t, result.Err())
		require.Equal(t, "c", result.Val())
		require.Equal(t, int64(2), rdb.LLen(ctx, key).Val())
	})

	t.Run("LPOP empty list", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.LPop(ctx, "{t}nosuchkey")
		require.Equal(t, redis.Nil, result.Err())
	})

	t.Run("RPOP empty list", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.RPop(ctx, "{t}nosuchkey")
		require.Equal(t, redis.Nil, result.Err())
	})

	t.Run("LPOP with count", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d", "e")
		result := rdb.LPopCount(ctx, key, 3)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"a", "b", "c"}, result.Val())
		require.Equal(t, int64(2), rdb.LLen(ctx, key).Val())
	})

	t.Run("RPOP with count", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d", "e")
		result := rdb.RPopCount(ctx, key, 3)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"e", "d", "c"}, result.Val())
		require.Equal(t, int64(2), rdb.LLen(ctx, key).Val())
	})

	t.Run("LINDEX positive index", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d")
		require.Equal(t, "a", rdb.LIndex(ctx, key, 0).Val())
		require.Equal(t, "b", rdb.LIndex(ctx, key, 1).Val())
		require.Equal(t, "c", rdb.LIndex(ctx, key, 2).Val())
		require.Equal(t, "d", rdb.LIndex(ctx, key, 3).Val())
	})

	t.Run("LINDEX negative index", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d")
		require.Equal(t, "d", rdb.LIndex(ctx, key, -1).Val())
		require.Equal(t, "c", rdb.LIndex(ctx, key, -2).Val())
		require.Equal(t, "a", rdb.LIndex(ctx, key, -4).Val())
	})

	t.Run("LINDEX out of range", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LIndex(ctx, key, 10)
		require.Equal(t, redis.Nil, result.Err())
		result = rdb.LIndex(ctx, key, -10)
		require.Equal(t, redis.Nil, result.Err())
	})

	t.Run("LRANGE basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d", "e")
		result := rdb.LRange(ctx, key, 1, 3)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"b", "c", "d"}, result.Val())
	})

	t.Run("LRANGE negative indices", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d", "e")
		result := rdb.LRange(ctx, key, -3, -1)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"c", "d", "e"}, result.Val())
	})

	t.Run("LRANGE out of bounds", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LRange(ctx, key, 0, 100)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"a", "b", "c"}, result.Val())
	})

	t.Run("LRANGE full list", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d")
		result := rdb.LRange(ctx, key, 0, -1)
		require.NoError(t, result.Err())
		require.Equal(t, []string{"a", "b", "c", "d"}, result.Val())
	})

	t.Run("LSET basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LSet(ctx, key, 1, "B")
		require.NoError(t, result.Err())
		require.Equal(t, "OK", result.Val())
		require.Equal(t, []string{"a", "B", "c"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("LSET negative index", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LSet(ctx, key, -1, "C")
		require.NoError(t, result.Err())
		require.Equal(t, "OK", result.Val())
		require.Equal(t, []string{"a", "b", "C"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("LSET out of range", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LSet(ctx, key, 10, "x")
		require.Error(t, result.Err())
	})

	t.Run("LSET non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.LSet(ctx, "{t}nosuchkey", 0, "x")
		require.Error(t, result.Err())
	})

	t.Run("LINSERT BEFORE", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LInsert(ctx, key, "BEFORE", "b", "X")
		require.NoError(t, result.Err())
		require.Equal(t, int64(4), result.Val())
		require.Equal(t, []string{"a", "X", "b", "c"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("LINSERT AFTER", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LInsert(ctx, key, "AFTER", "b", "X")
		require.NoError(t, result.Err())
		require.Equal(t, int64(4), result.Val())
		require.Equal(t, []string{"a", "b", "X", "c"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("LINSERT pivot not found", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LInsert(ctx, key, "BEFORE", "nonexistent", "X")
		require.NoError(t, result.Err())
		require.Equal(t, int64(-1), result.Val())
	})

	t.Run("LINSERT non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.LInsert(ctx, "{t}nosuchkey", "BEFORE", "a", "X")
		require.NoError(t, result.Err())
		require.Equal(t, int64(0), result.Val())
	})

	t.Run("LREM count > 0 removes from head", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "a", "c", "a")
		result := rdb.LRem(ctx, key, 2, "a")
		require.NoError(t, result.Err())
		require.Equal(t, int64(2), result.Val())
		require.Equal(t, []string{"b", "c", "a"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("LREM count < 0 removes from tail", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "a", "c", "a")
		result := rdb.LRem(ctx, key, -2, "a")
		require.NoError(t, result.Err())
		require.Equal(t, int64(2), result.Val())
		require.Equal(t, []string{"a", "b", "c"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("LREM count = 0 removes all", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "a", "c", "a")
		result := rdb.LRem(ctx, key, 0, "a")
		require.NoError(t, result.Err())
		require.Equal(t, int64(3), result.Val())
		require.Equal(t, []string{"b", "c"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("LTRIM basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d", "e")
		result := rdb.LTrim(ctx, key, 1, 3)
		require.NoError(t, result.Err())
		require.Equal(t, "OK", result.Val())
		require.Equal(t, []string{"b", "c", "d"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("LTRIM empty result", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LTrim(ctx, key, 5, 10)
		require.NoError(t, result.Err())
		require.Equal(t, "OK", result.Val())
		require.Equal(t, int64(0), rdb.LLen(ctx, key).Val())
	})

	t.Run("LPOS basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "b", "d")
		result := rdb.LPos(ctx, key, "b", redis.LPosArgs{})
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val())
	})

	t.Run("LPOS with RANK", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "b", "d")
		result := rdb.LPos(ctx, key, "b", redis.LPosArgs{Rank: 2})
		require.NoError(t, result.Err())
		require.Equal(t, int64(3), result.Val())
	})

	t.Run("LPOS element not found", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		result := rdb.LPos(ctx, key, "z", redis.LPosArgs{})
		require.Equal(t, redis.Nil, result.Err())
	})

	t.Run("LMOVE LEFT LEFT", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		src := "{t}src"
		dst := "{t}dst"
		rdb.RPush(ctx, src, "a", "b", "c")
		rdb.RPush(ctx, dst, "x", "y")
		result := rdb.LMove(ctx, src, dst, "left", "left")
		require.NoError(t, result.Err())
		require.Equal(t, "a", result.Val())
		require.Equal(t, []string{"b", "c"}, rdb.LRange(ctx, src, 0, -1).Val())
		require.Equal(t, []string{"a", "x", "y"}, rdb.LRange(ctx, dst, 0, -1).Val())
	})

	t.Run("LMOVE LEFT RIGHT", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		src := "{t}src"
		dst := "{t}dst"
		rdb.RPush(ctx, src, "a", "b", "c")
		rdb.RPush(ctx, dst, "x", "y")
		result := rdb.LMove(ctx, src, dst, "left", "right")
		require.NoError(t, result.Err())
		require.Equal(t, "a", result.Val())
		require.Equal(t, []string{"b", "c"}, rdb.LRange(ctx, src, 0, -1).Val())
		require.Equal(t, []string{"x", "y", "a"}, rdb.LRange(ctx, dst, 0, -1).Val())
	})

	t.Run("LMOVE same key rotates list", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c", "d")
		result := rdb.LMove(ctx, key, key, "left", "right")
		require.NoError(t, result.Err())
		require.Equal(t, "a", result.Val())
		require.Equal(t, []string{"b", "c", "d", "a"}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("TYPE returns list for list key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.LPush(ctx, key, "hello")
		require.Equal(t, "list", rdb.Type(ctx, key).Val())
	})

	t.Run("DEL removes list key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mylist"
		rdb.RPush(ctx, key, "a", "b", "c")
		require.Equal(t, int64(1), rdb.Del(ctx, key).Val())
		require.Equal(t, int64(0), rdb.LLen(ctx, key).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	})

	t.Run("WRONGTYPE error on LPUSH to string key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}mystring"
		rdb.Set(ctx, key, "hello", 0)
		result := rdb.LPush(ctx, key, "world")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "WRONGTYPE")
	})
}
