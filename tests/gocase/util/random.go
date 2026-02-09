// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package util

import "math/rand"

// RandString generates a random string of length between min and max.
func RandString(min, max int) string {
	n := min + rand.Intn(max-min+1)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(48 + rand.Intn(75)) // ASCII 48-122
	}
	return string(b)
}

// RandomInt returns a random int in [0, maxInt).
func RandomInt(maxInt int64) int64 {
	return rand.Int63n(maxInt)
}
