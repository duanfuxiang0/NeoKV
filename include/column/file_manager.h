// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub file_manager for neo-redis

#pragma once

namespace baikaldb {

class FileManager {
public:
    static FileManager* get_instance() {
        static FileManager instance;
        return &instance;
    }
};

} // namespace baikaldb
