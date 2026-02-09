// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package set

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/neokv/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SADD single member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		result := rdb.SAdd(ctx, key, "hello")
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val())
	})

	t.Run("SADD multiple members", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		result := rdb.SAdd(ctx, key, "a", "b", "c")
		require.NoError(t, result.Err())
		require.Equal(t, int64(3), result.Val())
	})

	t.Run("SADD duplicate members", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		require.Equal(t, int64(3), rdb.SAdd(ctx, key, "a", "b", "c").Val())
		// Adding existing members returns 0
		require.Equal(t, int64(0), rdb.SAdd(ctx, key, "a", "b").Val())
		// Adding mix of new and existing
		require.Equal(t, int64(2), rdb.SAdd(ctx, key, "b", "d", "c", "e").Val())
	})

	t.Run("SADD duplicate members in same call", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		// Duplicates within the same SADD call should be deduplicated
		require.Equal(t, int64(2), rdb.SAdd(ctx, key, "a", "b", "a", "b").Val())
	})

	t.Run("SCARD", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		require.Equal(t, int64(0), rdb.SCard(ctx, key).Val())
		rdb.SAdd(ctx, key, "a", "b", "c")
		require.Equal(t, int64(3), rdb.SCard(ctx, key).Val())
	})

	t.Run("SCARD non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.Equal(t, int64(0), rdb.SCard(ctx, "{t}nosuchkey").Val())
	})

	t.Run("SISMEMBER", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "hello", "world")
		require.True(t, rdb.SIsMember(ctx, key, "hello").Val())
		require.True(t, rdb.SIsMember(ctx, key, "world").Val())
		require.False(t, rdb.SIsMember(ctx, key, "missing").Val())
	})

	t.Run("SISMEMBER non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.False(t, rdb.SIsMember(ctx, "{t}nosuchkey", "member").Val())
	})

	t.Run("SMEMBERS", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "hello", "world", "foo")
		members := rdb.SMembers(ctx, key).Val()
		sort.Strings(members)
		require.Equal(t, []string{"foo", "hello", "world"}, members)
	})

	t.Run("SMEMBERS empty set", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		members := rdb.SMembers(ctx, "{t}nosuchkey").Val()
		require.Equal(t, 0, len(members))
	})

	t.Run("SREM single member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		result := rdb.SRem(ctx, key, "b")
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val())
		members := rdb.SMembers(ctx, key).Val()
		sort.Strings(members)
		require.Equal(t, []string{"a", "c"}, members)
	})

	t.Run("SREM multiple members", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c", "d")
		result := rdb.SRem(ctx, key, "b", "d")
		require.NoError(t, result.Err())
		require.Equal(t, int64(2), result.Val())
		members := rdb.SMembers(ctx, key).Val()
		sort.Strings(members)
		require.Equal(t, []string{"a", "c"}, members)
	})

	t.Run("SREM non-existing member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b")
		require.Equal(t, int64(0), rdb.SRem(ctx, key, "nonexistent").Val())
	})

	t.Run("SREM non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.Equal(t, int64(0), rdb.SRem(ctx, "{t}nosuchkey", "member").Val())
	})

	t.Run("SREM all members removes key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b")
		rdb.SRem(ctx, key, "a", "b")
		require.Equal(t, int64(0), rdb.SCard(ctx, key).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	})

	t.Run("SPOP single element", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		popped := rdb.SPop(ctx, key).Val()
		require.Contains(t, []string{"a", "b", "c"}, popped)
		require.Equal(t, int64(2), rdb.SCard(ctx, key).Val())
		require.False(t, rdb.SIsMember(ctx, key, popped).Val())
	})

	t.Run("SPOP with count", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c", "d", "e")
		popped := rdb.SPopN(ctx, key, 3).Val()
		require.Equal(t, 3, len(popped))
		require.Equal(t, int64(2), rdb.SCard(ctx, key).Val())
		// All popped members should no longer be in the set
		for _, m := range popped {
			require.False(t, rdb.SIsMember(ctx, key, m).Val())
		}
	})

	t.Run("SPOP count greater than set size", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b")
		popped := rdb.SPopN(ctx, key, 10).Val()
		require.Equal(t, 2, len(popped))
		sort.Strings(popped)
		require.Equal(t, []string{"a", "b"}, popped)
		require.Equal(t, int64(0), rdb.SCard(ctx, key).Val())
	})

	t.Run("SPOP empty set", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.SPop(ctx, "{t}nosuchkey")
		require.Equal(t, redis.Nil, result.Err())
	})

	t.Run("SRANDMEMBER without count", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		member := rdb.SRandMember(ctx, key).Val()
		require.Contains(t, []string{"a", "b", "c"}, member)
		// Set should not be modified
		require.Equal(t, int64(3), rdb.SCard(ctx, key).Val())
	})

	t.Run("SRANDMEMBER with positive count", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c", "d", "e")
		members := rdb.SRandMemberN(ctx, key, 3).Val()
		require.Equal(t, 3, len(members))
		// All returned members should be unique
		seen := make(map[string]bool)
		for _, m := range members {
			require.False(t, seen[m], "duplicate member in SRANDMEMBER result")
			seen[m] = true
			require.Contains(t, []string{"a", "b", "c", "d", "e"}, m)
		}
		// Set should not be modified
		require.Equal(t, int64(5), rdb.SCard(ctx, key).Val())
	})

	t.Run("SRANDMEMBER with count greater than set size", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		members := rdb.SRandMemberN(ctx, key, 10).Val()
		sort.Strings(members)
		require.Equal(t, []string{"a", "b", "c"}, members)
	})

	t.Run("SRANDMEMBER with negative count allows duplicates", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a")
		// With negative count on a single-element set, all results should be "a"
		members := rdb.SRandMemberN(ctx, key, -5).Val()
		require.Equal(t, 5, len(members))
		for _, m := range members {
			require.Equal(t, "a", m)
		}
	})

	t.Run("SRANDMEMBER empty set", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.SRandMember(ctx, "{t}nosuchkey")
		require.Equal(t, redis.Nil, result.Err())
	})

	t.Run("DEL removes set key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		require.Equal(t, int64(1), rdb.Del(ctx, key).Val())
		require.Equal(t, int64(0), rdb.SCard(ctx, key).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	})

	t.Run("EXISTS works with set key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
		rdb.SAdd(ctx, key, "hello")
		require.Equal(t, int64(1), rdb.Exists(ctx, key).Val())
	})

	t.Run("TYPE returns set for set key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "hello")
		require.Equal(t, "set", rdb.Type(ctx, key).Val())
	})

	t.Run("SADD large number of members", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}bigset"
		members := make([]interface{}, 100)
		for i := 0; i < 100; i++ {
			members[i] = "member" + strconv.Itoa(i)
		}
		result := rdb.SAdd(ctx, key, members...)
		require.NoError(t, result.Err())
		require.Equal(t, int64(100), result.Val())
		require.Equal(t, int64(100), rdb.SCard(ctx, key).Val())
	})

	t.Run("FLUSHDB clears set keys", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.Equal(t, int64(0), rdb.SCard(ctx, key).Val())
	})

	t.Run("SADD and SREM interleaved", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		rdb.SRem(ctx, key, "b")
		rdb.SAdd(ctx, key, "d", "e")
		rdb.SRem(ctx, key, "a", "c")
		members := rdb.SMembers(ctx, key).Val()
		sort.Strings(members)
		require.Equal(t, []string{"d", "e"}, members)
		require.Equal(t, int64(2), rdb.SCard(ctx, key).Val())
	})

	t.Run("SPOP all elements empties set", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		popped := rdb.SPopN(ctx, key, 3).Val()
		require.Equal(t, 3, len(popped))
		require.Equal(t, int64(0), rdb.SCard(ctx, key).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	})

	t.Run("SADD with empty string member", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		require.Equal(t, int64(1), rdb.SAdd(ctx, key, "").Val())
		require.True(t, rdb.SIsMember(ctx, key, "").Val())
		members := rdb.SMembers(ctx, key).Val()
		require.Equal(t, []string{""}, members)
	})

	// ================================================================
	// SMISMEMBER tests
	// ================================================================

	t.Run("SMISMEMBER basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c")
		result := rdb.SMIsMember(ctx, key, "a", "b", "d", "c").Val()
		require.Equal(t, []bool{true, true, false, true}, result)
	})

	t.Run("SMISMEMBER non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		result := rdb.SMIsMember(ctx, "{t}nosuchkey", "a", "b").Val()
		require.Equal(t, []bool{false, false}, result)
	})

	// ================================================================
	// SSCAN tests
	// ================================================================

	t.Run("SSCAN basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "a", "b", "c", "d")
		members, cursor, err := rdb.SScan(ctx, key, 0, "", 100).Result()
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
		sort.Strings(members)
		require.Equal(t, []string{"a", "b", "c", "d"}, members)
	})

	t.Run("SSCAN with MATCH pattern", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		key := "{t}myset"
		rdb.SAdd(ctx, key, "apple", "banana", "avocado", "blueberry")
		members, cursor, err := rdb.SScan(ctx, key, 0, "a*", 100).Result()
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
		sort.Strings(members)
		require.Equal(t, []string{"apple", "avocado"}, members)
	})

	t.Run("SSCAN non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		members, cursor, err := rdb.SScan(ctx, "{t}nosuchkey", 0, "", 100).Result()
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
		require.Equal(t, 0, len(members))
	})

	// ================================================================
	// SINTER tests
	// ================================================================

	t.Run("SINTER two sets", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c", "d")
		rdb.SAdd(ctx, "{t}s2", "b", "c", "e")
		result := rdb.SInter(ctx, "{t}s1", "{t}s2").Val()
		sort.Strings(result)
		require.Equal(t, []string{"b", "c"}, result)
	})

	t.Run("SINTER three sets", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c")
		rdb.SAdd(ctx, "{t}s2", "b", "c", "d")
		rdb.SAdd(ctx, "{t}s3", "c", "d", "e")
		result := rdb.SInter(ctx, "{t}s1", "{t}s2", "{t}s3").Val()
		require.Equal(t, []string{"c"}, result)
	})

	t.Run("SINTER with non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		result := rdb.SInter(ctx, "{t}s1", "{t}nosuchkey").Val()
		require.Equal(t, 0, len(result))
	})

	t.Run("SINTER single set", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c")
		result := rdb.SInter(ctx, "{t}s1").Val()
		sort.Strings(result)
		require.Equal(t, []string{"a", "b", "c"}, result)
	})

	// ================================================================
	// SUNION tests
	// ================================================================

	t.Run("SUNION two sets", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		rdb.SAdd(ctx, "{t}s2", "b", "c")
		result := rdb.SUnion(ctx, "{t}s1", "{t}s2").Val()
		sort.Strings(result)
		require.Equal(t, []string{"a", "b", "c"}, result)
	})

	t.Run("SUNION with non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		result := rdb.SUnion(ctx, "{t}s1", "{t}nosuchkey").Val()
		sort.Strings(result)
		require.Equal(t, []string{"a", "b"}, result)
	})

	t.Run("SUNION three sets with overlap", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		rdb.SAdd(ctx, "{t}s2", "b", "c")
		rdb.SAdd(ctx, "{t}s3", "c", "d")
		result := rdb.SUnion(ctx, "{t}s1", "{t}s2", "{t}s3").Val()
		sort.Strings(result)
		require.Equal(t, []string{"a", "b", "c", "d"}, result)
	})

	// ================================================================
	// SDIFF tests
	// ================================================================

	t.Run("SDIFF two sets", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c", "d")
		rdb.SAdd(ctx, "{t}s2", "b", "d")
		result := rdb.SDiff(ctx, "{t}s1", "{t}s2").Val()
		sort.Strings(result)
		require.Equal(t, []string{"a", "c"}, result)
	})

	t.Run("SDIFF with non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		result := rdb.SDiff(ctx, "{t}s1", "{t}nosuchkey").Val()
		sort.Strings(result)
		require.Equal(t, []string{"a", "b"}, result)
	})

	t.Run("SDIFF first key non-existing", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s2", "a", "b")
		result := rdb.SDiff(ctx, "{t}nosuchkey", "{t}s2").Val()
		require.Equal(t, 0, len(result))
	})

	t.Run("SDIFF three sets", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c", "d")
		rdb.SAdd(ctx, "{t}s2", "b")
		rdb.SAdd(ctx, "{t}s3", "c")
		result := rdb.SDiff(ctx, "{t}s1", "{t}s2", "{t}s3").Val()
		sort.Strings(result)
		require.Equal(t, []string{"a", "d"}, result)
	})

	// ================================================================
	// SINTERCARD tests
	// ================================================================

	t.Run("SINTERCARD basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c", "d")
		rdb.SAdd(ctx, "{t}s2", "b", "c", "e")
		result := rdb.SInterCard(ctx, 0, "{t}s1", "{t}s2").Val()
		require.Equal(t, int64(2), result)
	})

	t.Run("SINTERCARD with LIMIT", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c", "d")
		rdb.SAdd(ctx, "{t}s2", "b", "c", "d", "e")
		// Intersection is {b, c, d} = 3, but limit to 1
		result := rdb.SInterCard(ctx, 1, "{t}s1", "{t}s2").Val()
		require.Equal(t, int64(1), result)
	})

	t.Run("SINTERCARD with non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		result := rdb.SInterCard(ctx, 0, "{t}s1", "{t}nosuchkey").Val()
		require.Equal(t, int64(0), result)
	})

	// ================================================================
	// SMOVE tests
	// ================================================================

	t.Run("SMOVE member exists in source", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}src", "a", "b", "c")
		rdb.SAdd(ctx, "{t}dst", "x", "y")
		result := rdb.SMove(ctx, "{t}src", "{t}dst", "b").Val()
		require.True(t, result)
		// b should be removed from source
		require.False(t, rdb.SIsMember(ctx, "{t}src", "b").Val())
		require.Equal(t, int64(2), rdb.SCard(ctx, "{t}src").Val())
		// b should be added to destination
		require.True(t, rdb.SIsMember(ctx, "{t}dst", "b").Val())
		require.Equal(t, int64(3), rdb.SCard(ctx, "{t}dst").Val())
	})

	t.Run("SMOVE member not in source", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}src", "a", "b")
		rdb.SAdd(ctx, "{t}dst", "x")
		result := rdb.SMove(ctx, "{t}src", "{t}dst", "z").Val()
		require.False(t, result)
		require.Equal(t, int64(2), rdb.SCard(ctx, "{t}src").Val())
		require.Equal(t, int64(1), rdb.SCard(ctx, "{t}dst").Val())
	})

	t.Run("SMOVE source does not exist", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}dst", "x")
		result := rdb.SMove(ctx, "{t}nosuchkey", "{t}dst", "a").Val()
		require.False(t, result)
	})

	t.Run("SMOVE to non-existing destination", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}src", "a", "b")
		result := rdb.SMove(ctx, "{t}src", "{t}newdst", "a").Val()
		require.True(t, result)
		require.Equal(t, int64(1), rdb.SCard(ctx, "{t}src").Val())
		require.True(t, rdb.SIsMember(ctx, "{t}newdst", "a").Val())
		require.Equal(t, int64(1), rdb.SCard(ctx, "{t}newdst").Val())
	})

	t.Run("SMOVE member already in destination", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}src", "a", "b")
		rdb.SAdd(ctx, "{t}dst", "a", "x")
		result := rdb.SMove(ctx, "{t}src", "{t}dst", "a").Val()
		require.True(t, result)
		require.Equal(t, int64(1), rdb.SCard(ctx, "{t}src").Val())
		// Destination should still have 2 members (a was already there)
		require.Equal(t, int64(2), rdb.SCard(ctx, "{t}dst").Val())
	})

	// ================================================================
	// SINTERSTORE tests
	// ================================================================

	t.Run("SINTERSTORE basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c")
		rdb.SAdd(ctx, "{t}s2", "b", "c", "d")
		result := rdb.SInterStore(ctx, "{t}dest", "{t}s1", "{t}s2").Val()
		require.Equal(t, int64(2), result)
		members := rdb.SMembers(ctx, "{t}dest").Val()
		sort.Strings(members)
		require.Equal(t, []string{"b", "c"}, members)
	})

	t.Run("SINTERSTORE with non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		result := rdb.SInterStore(ctx, "{t}dest", "{t}s1", "{t}nosuchkey").Val()
		require.Equal(t, int64(0), result)
		require.Equal(t, int64(0), rdb.Exists(ctx, "{t}dest").Val())
	})

	t.Run("SINTERSTORE overwrites existing destination", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}dest", "old1", "old2", "old3")
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c")
		rdb.SAdd(ctx, "{t}s2", "b", "c", "d")
		result := rdb.SInterStore(ctx, "{t}dest", "{t}s1", "{t}s2").Val()
		require.Equal(t, int64(2), result)
		members := rdb.SMembers(ctx, "{t}dest").Val()
		sort.Strings(members)
		require.Equal(t, []string{"b", "c"}, members)
	})

	// ================================================================
	// SUNIONSTORE tests
	// ================================================================

	t.Run("SUNIONSTORE basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		rdb.SAdd(ctx, "{t}s2", "b", "c")
		result := rdb.SUnionStore(ctx, "{t}dest", "{t}s1", "{t}s2").Val()
		require.Equal(t, int64(3), result)
		members := rdb.SMembers(ctx, "{t}dest").Val()
		sort.Strings(members)
		require.Equal(t, []string{"a", "b", "c"}, members)
	})

	t.Run("SUNIONSTORE with non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b")
		result := rdb.SUnionStore(ctx, "{t}dest", "{t}s1", "{t}nosuchkey").Val()
		require.Equal(t, int64(2), result)
		members := rdb.SMembers(ctx, "{t}dest").Val()
		sort.Strings(members)
		require.Equal(t, []string{"a", "b"}, members)
	})

	// ================================================================
	// SDIFFSTORE tests
	// ================================================================

	t.Run("SDIFFSTORE basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c", "d")
		rdb.SAdd(ctx, "{t}s2", "b", "d")
		result := rdb.SDiffStore(ctx, "{t}dest", "{t}s1", "{t}s2").Val()
		require.Equal(t, int64(2), result)
		members := rdb.SMembers(ctx, "{t}dest").Val()
		sort.Strings(members)
		require.Equal(t, []string{"a", "c"}, members)
	})

	t.Run("SDIFFSTORE with non-existing first key", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}s2", "a", "b")
		result := rdb.SDiffStore(ctx, "{t}dest", "{t}nosuchkey", "{t}s2").Val()
		require.Equal(t, int64(0), result)
		require.Equal(t, int64(0), rdb.Exists(ctx, "{t}dest").Val())
	})

	t.Run("SDIFFSTORE overwrites existing destination", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		rdb.SAdd(ctx, "{t}dest", "old1", "old2")
		rdb.SAdd(ctx, "{t}s1", "a", "b", "c")
		rdb.SAdd(ctx, "{t}s2", "b")
		result := rdb.SDiffStore(ctx, "{t}dest", "{t}s1", "{t}s2").Val()
		require.Equal(t, int64(2), result)
		members := rdb.SMembers(ctx, "{t}dest").Val()
		sort.Strings(members)
		require.Equal(t, []string{"a", "c"}, members)
	})
}
