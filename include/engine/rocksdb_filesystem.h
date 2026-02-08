// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub rocksdb_filesystem for neo-redis

#pragma once

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"

namespace neokv {

// Simplified RocksdbFileSystemWrapper - uses default filesystem
// Cold storage feature is disabled for neo-redis
class RocksdbFileSystemWrapper : public rocksdb::FileSystemWrapper {
public:
	explicit RocksdbFileSystemWrapper(bool /*is_cold*/ = false)
	    : rocksdb::FileSystemWrapper(rocksdb::FileSystem::Default()) {
	}

	const char* Name() const override {
		return "RocksdbFileSystemWrapper";
	}

	static const char* kClassName() {
		return "RocksdbFileSystemWrapper";
	}
};

} // namespace neokv
