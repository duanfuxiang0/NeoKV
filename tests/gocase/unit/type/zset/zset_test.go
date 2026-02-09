package zset

import (
	"context"
	"sort"
	"testing"

	"github.com/neokv/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestZSet(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	// ================================================================
	// ZADD / ZCARD / ZSCORE basics
	// ================================================================

	t.Run("ZADD single member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1.0, Member: "a"})
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val())
	})

	t.Run("ZADD multiple members", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		require.NoError(t, result.Err())
		require.Equal(t, int64(3), result.Val())
	})

	t.Run("ZADD update existing score", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"})
		// Update score — returns 0 (no new members added)
		result := rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 5, Member: "a"})
		require.Equal(t, int64(0), result.Val())
		score := rdb.ZScore(ctx, "{t}zs", "a").Val()
		require.Equal(t, 5.0, score)
	})

	t.Run("ZCARD", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.Equal(t, int64(0), rdb.ZCard(ctx, "{t}zs").Val())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		require.Equal(t, int64(2), rdb.ZCard(ctx, "{t}zs").Val())
	})

	t.Run("ZSCORE existing member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1.5, Member: "a"})
		score, err := rdb.ZScore(ctx, "{t}zs", "a").Result()
		require.NoError(t, err)
		require.Equal(t, 1.5, score)
	})

	t.Run("ZSCORE non-existing member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"})
		_, err := rdb.ZScore(ctx, "{t}zs", "b").Result()
		require.Equal(t, redis.Nil, err)
	})

	// ================================================================
	// ZREM
	// ================================================================

	t.Run("ZREM single member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		result := rdb.ZRem(ctx, "{t}zs", "b")
		require.Equal(t, int64(1), result.Val())
		require.Equal(t, int64(2), rdb.ZCard(ctx, "{t}zs").Val())
	})

	t.Run("ZREM non-existing member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"})
		require.Equal(t, int64(0), rdb.ZRem(ctx, "{t}zs", "z").Val())
	})

	t.Run("ZREM all members removes key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		rdb.ZRem(ctx, "{t}zs", "a", "b")
		require.Equal(t, int64(0), rdb.ZCard(ctx, "{t}zs").Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, "{t}zs").Val())
	})

	// ================================================================
	// ZINCRBY
	// ================================================================

	t.Run("ZINCRBY existing member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 10, Member: "a"})
		result := rdb.ZIncrBy(ctx, "{t}zs", 5, "a")
		require.NoError(t, result.Err())
		require.Equal(t, 15.0, result.Val())
		require.Equal(t, 15.0, rdb.ZScore(ctx, "{t}zs", "a").Val())
	})

	t.Run("ZINCRBY new member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.ZIncrBy(ctx, "{t}zs", 3.5, "newmember")
		require.Equal(t, 3.5, result.Val())
		require.Equal(t, int64(1), rdb.ZCard(ctx, "{t}zs").Val())
	})

	// ================================================================
	// ZRANK / ZREVRANK
	// ================================================================

	t.Run("ZRANK", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		require.Equal(t, int64(0), rdb.ZRank(ctx, "{t}zs", "a").Val())
		require.Equal(t, int64(1), rdb.ZRank(ctx, "{t}zs", "b").Val())
		require.Equal(t, int64(2), rdb.ZRank(ctx, "{t}zs", "c").Val())
	})

	t.Run("ZRANK non-existing member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"})
		_, err := rdb.ZRank(ctx, "{t}zs", "z").Result()
		require.Equal(t, redis.Nil, err)
	})

	t.Run("ZREVRANK", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		require.Equal(t, int64(2), rdb.ZRevRank(ctx, "{t}zs", "a").Val())
		require.Equal(t, int64(1), rdb.ZRevRank(ctx, "{t}zs", "b").Val())
		require.Equal(t, int64(0), rdb.ZRevRank(ctx, "{t}zs", "c").Val())
	})

	// ================================================================
	// ZRANGE
	// ================================================================

	t.Run("ZRANGE by rank", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		result := rdb.ZRange(ctx, "{t}zs", 0, -1).Val()
		require.Equal(t, []string{"a", "b", "c"}, result)
	})

	t.Run("ZRANGE partial range", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}, redis.Z{Score: 4, Member: "d"})
		result := rdb.ZRange(ctx, "{t}zs", 1, 2).Val()
		require.Equal(t, []string{"b", "c"}, result)
	})

	t.Run("ZRANGE with WITHSCORES", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		result := rdb.ZRangeWithScores(ctx, "{t}zs", 0, -1).Val()
		require.Equal(t, 2, len(result))
		require.Equal(t, "a", result[0].Member)
		require.Equal(t, 1.0, result[0].Score)
		require.Equal(t, "b", result[1].Member)
		require.Equal(t, 2.0, result[1].Score)
	})

	// ================================================================
	// ZREVRANGE
	// ================================================================

	t.Run("ZREVRANGE", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		result := rdb.ZRevRange(ctx, "{t}zs", 0, -1).Val()
		require.Equal(t, []string{"c", "b", "a"}, result)
	})

	// ================================================================
	// ZRANGEBYSCORE / ZREVRANGEBYSCORE
	// ================================================================

	t.Run("ZRANGEBYSCORE", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}, redis.Z{Score: 4, Member: "d"})
		result := rdb.ZRangeByScore(ctx, "{t}zs", &redis.ZRangeBy{Min: "2", Max: "3"}).Val()
		require.Equal(t, []string{"b", "c"}, result)
	})

	t.Run("ZRANGEBYSCORE with -inf +inf", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		result := rdb.ZRangeByScore(ctx, "{t}zs", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Val()
		require.Equal(t, []string{"a", "b"}, result)
	})

	t.Run("ZRANGEBYSCORE exclusive", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		result := rdb.ZRangeByScore(ctx, "{t}zs", &redis.ZRangeBy{Min: "(1", Max: "(3"}).Val()
		require.Equal(t, []string{"b"}, result)
	})

	t.Run("ZREVRANGEBYSCORE", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		result := rdb.ZRevRangeByScore(ctx, "{t}zs", &redis.ZRangeBy{Min: "1", Max: "3"}).Val()
		require.Equal(t, []string{"c", "b", "a"}, result)
	})

	// ================================================================
	// ZCOUNT
	// ================================================================

	t.Run("ZCOUNT", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}, redis.Z{Score: 4, Member: "d"})
		require.Equal(t, int64(3), rdb.ZCount(ctx, "{t}zs", "1", "3").Val())
		require.Equal(t, int64(4), rdb.ZCount(ctx, "{t}zs", "-inf", "+inf").Val())
		require.Equal(t, int64(1), rdb.ZCount(ctx, "{t}zs", "(1", "(3").Val())
	})

	// ================================================================
	// ZPOPMIN / ZPOPMAX
	// ================================================================

	t.Run("ZPOPMIN", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		result := rdb.ZPopMin(ctx, "{t}zs", 2).Val()
		require.Equal(t, 2, len(result))
		require.Equal(t, "a", result[0].Member)
		require.Equal(t, 1.0, result[0].Score)
		require.Equal(t, "b", result[1].Member)
		require.Equal(t, 2.0, result[1].Score)
		require.Equal(t, int64(1), rdb.ZCard(ctx, "{t}zs").Val())
	})

	t.Run("ZPOPMAX", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		result := rdb.ZPopMax(ctx, "{t}zs", 2).Val()
		require.Equal(t, 2, len(result))
		require.Equal(t, "c", result[0].Member)
		require.Equal(t, 3.0, result[0].Score)
		require.Equal(t, "b", result[1].Member)
		require.Equal(t, 2.0, result[1].Score)
		require.Equal(t, int64(1), rdb.ZCard(ctx, "{t}zs").Val())
	})

	t.Run("ZPOPMIN empty set", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.ZPopMin(ctx, "{t}zs").Val()
		require.Equal(t, 0, len(result))
	})

	// ================================================================
	// ZSCAN
	// ================================================================

	t.Run("ZSCAN basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		members, cursor, err := rdb.ZScan(ctx, "{t}zs", 0, "", 100).Result()
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
		// ZSCAN returns [member, score, member, score, ...]
		require.Equal(t, 4, len(members))
	})

	// ================================================================
	// TYPE
	// ================================================================

	t.Run("TYPE returns zset", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"})
		require.Equal(t, "zset", rdb.Type(ctx, "{t}zs").Val())
	})

	// ================================================================
	// ZRANGEBYLEX / ZLEXCOUNT
	// ================================================================

	t.Run("ZRANGEBYLEX", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		// All members with same score for lex ordering
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"}, redis.Z{Score: 0, Member: "c"}, redis.Z{Score: 0, Member: "d"})
		result := rdb.ZRangeByLex(ctx, "{t}zs", &redis.ZRangeBy{Min: "[b", Max: "[d"}).Val()
		sort.Strings(result)
		require.Equal(t, []string{"b", "c", "d"}, result)
	})

	t.Run("ZLEXCOUNT", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"}, redis.Z{Score: 0, Member: "c"})
		require.Equal(t, int64(3), rdb.ZLexCount(ctx, "{t}zs", "-", "+").Val())
		require.Equal(t, int64(2), rdb.ZLexCount(ctx, "{t}zs", "[a", "[b").Val())
	})

	// ================================================================
	// DEL / FLUSHDB
	// ================================================================

	t.Run("DEL removes zset key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.ZAdd(ctx, "{t}zs", redis.Z{Score: 1, Member: "a"})
		require.Equal(t, int64(1), rdb.Del(ctx, "{t}zs").Val())
		require.Equal(t, int64(0), rdb.ZCard(ctx, "{t}zs").Val())
	})
}
