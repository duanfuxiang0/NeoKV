// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package ping

import (
	"testing"

	"github.com/neokv/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestPing(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	t.Run("PING returns PONG", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("PING"))
		c.MustRead(t, "+PONG")
	})

	t.Run("PING with message returns bulk string", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("PING", "hello"))
		c.MustRead(t, "$5")
		c.MustRead(t, "hello")
	})

	t.Run("PING via go-redis client", func(t *testing.T) {
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		pong, err := rdb.Ping(t.Context()).Result()
		require.NoError(t, err)
		require.Equal(t, "PONG", pong)
	})
}
