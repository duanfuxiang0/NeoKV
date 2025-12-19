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
    
    // Constructor from existing string
    explicit MutTableKey(const std::string& data) : _data(data) {}
    
    // Constructor with is_full_key flag (for compatibility)
    MutTableKey(const std::string& data, bool /*is_full_key*/) : _data(data) {}
    
    MutTableKey& append_i64(int64_t val) {
        KeyEncoder::append_i64(_data, val);
        return *this;
    }
    
    MutTableKey& append_u64(uint64_t val) {
        KeyEncoder::append_u64(_data, val);
        return *this;
    }
    
    MutTableKey& append_u32(uint32_t val) {
        KeyEncoder::append_u32(_data, val);
        return *this;
    }
    
    MutTableKey& append_u16(uint16_t val) {
        KeyEncoder::append_u16(_data, val);
        return *this;
    }
    
    MutTableKey& append_u8(uint8_t val) {
        KeyEncoder::append_u8(_data, val);
        return *this;
    }
    
    MutTableKey& append_index(const std::string& key) {
        _data.append(key);
        return *this;
    }
    
    MutTableKey& append_char(char c) {
        _data.push_back(c);
        return *this;
    }
    
    // Overload for append_char(const char* data, size_t len)
    MutTableKey& append_char(const char* data, size_t len) {
        _data.append(data, len);
        return *this;
    }
    
    MutTableKey& append_string(const std::string& str) {
        _data.append(str);
        return *this;
    }
    
    const std::string& data() const {
        return _data;
    }
    
    std::string& data() {
        return _data;
    }
    
    void clear() {
        _data.clear();
    }
    
    size_t size() const {
        return _data.size();
    }
    
    // Replace replace_u64 - replaces at position pos
    void replace_u64(uint64_t val, size_t pos) {
        if (pos + 8 > _data.size()) {
            return;
        }
        uint64_t be = KeyEncoder::to_endian_u64(val);
        memcpy(&_data[pos], &be, sizeof(uint64_t));
    }
    
    void replace_i64(int64_t val, size_t pos) {
        if (pos + 8 > _data.size()) {
            return;
        }
        uint64_t encoded = KeyEncoder::encode_i64(val);
        uint64_t be = KeyEncoder::to_endian_u64(encoded);
        memcpy(&_data[pos], &be, sizeof(uint64_t));
    }

private:
    std::string _data;
};

} // namespace baikaldb
