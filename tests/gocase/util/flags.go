// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package util

import "flag"

var binPath = flag.String("binPath", "", "path to neo_redis_standalone binary")
var workspace = flag.String("workspace", "", "directory of cases workspace")
var deleteOnExit = flag.Bool("deleteOnExit", true, "whether to delete workspace on exit")
