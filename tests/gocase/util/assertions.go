// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

// BetweenValues asserts that start <= d <= end (inclusive).
func BetweenValues[T constraints.Ordered](t testing.TB, d, start, end T) {
	t.Helper()
	require.GreaterOrEqual(t, d, start)
	require.LessOrEqual(t, d, end)
}

// BetweenValuesEx asserts that start < d < end (exclusive).
func BetweenValuesEx[T constraints.Ordered](t testing.TB, d, start, end T) {
	t.Helper()
	require.Greater(t, d, start)
	require.Less(t, d, end)
}
