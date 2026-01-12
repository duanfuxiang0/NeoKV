// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Simplified TableKey for neo-redis

#pragma once

#include <string>
#include <cstdint>
#include "rocksdb/slice.h"
#include "key_encoder.h"

namespace neokv {

// Simplified TableKey for reading keys
class TableKey {
public:
    TableKey() = default;
    
    TableKey(const rocksdb::Slice& key) : _data(key) {}
    
    TableKey(const std::string& key) : _data(key) {}
    
    int64_t extract_i64(int pos) const {
        if (pos + 8 > (int)_data.size()) {
            return 0;
        }
        uint64_t val;
        memcpy(&val, _data.data() + pos, sizeof(val));
        return (int64_t)KeyEncoder::to_endian_u64(val);
    }
    
    uint64_t extract_u64(int pos) const {
        if (pos + 8 > (int)_data.size()) {
            return 0;
        }
        uint64_t val;
        memcpy(&val, _data.data() + pos, sizeof(val));
        return KeyEncoder::to_endian_u64(val);
    }
    
    uint16_t extract_u16(int pos) const {
        if (pos + 2 > (int)_data.size()) {
            return 0;
        }
        uint16_t val;
        memcpy(&val, _data.data() + pos, sizeof(val));
        return KeyEncoder::to_endian_u16(val);
    }
    
    uint8_t extract_u8(int pos) const {
        if (pos >= (int)_data.size()) {
            return 0;
        }
        return static_cast<uint8_t>(_data.data()[pos]);
    }
    
    // Extract a string/char array
    void extract_char(int pos, int len, std::string& out) const {
        if (pos + len > (int)_data.size()) {
            out.clear();
            return;
        }
        out.assign(_data.data() + pos, len);
    }
    
    rocksdb::Slice data() const {
        return _data;
    }
    
    size_t size() const {
        return _data.size();
    }
    
private:
    rocksdb::Slice _data;
};

} // namespace neokv
