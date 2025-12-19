// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Simplified MutableKey for neo-redis

#pragma once

#include <string>
#include <cstdint>
#include "key_encoder.h"

namespace baikaldb {

// Simplified MutableKey for region/raft key encoding
class MutTableKey {
public:
    MutTableKey() = default;
    
    void append_i64(int64_t val) {
        KeyEncoder::append_i64(_data, val);
    }
    
    void append_u64(uint64_t val) {
        KeyEncoder::append_u64(_data, val);
    }
    
    void append_u16(uint16_t val) {
        KeyEncoder::append_u16(_data, val);
    }
    
    void append_u8(uint8_t val) {
        KeyEncoder::append_u8(_data, val);
    }
    
    void append_index(const std::string& key) {
        _data.append(key);
    }
    
    void append_char(char c) {
        _data.push_back(c);
    }
    
    const std::string& data() const {
        return _data;
    }
    
    void clear() {
        _data.clear();
    }
    
    size_t size() const {
        return _data.size();
    }
    
private:
    std::string _data;
};

} // namespace baikaldb
