// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Simplified key encoder for neo-redis

#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <arpa/inet.h>
#include "rocksdb/slice.h"

namespace neokv {

class KeyEncoder {
public:
	// ========================================================================
	// Byte-order conversion utilities
	// ========================================================================
	// NOTE: to_endian_u{16,32,64} are self-inverse (swap between host and
	// big-endian byte order). The semantic aliases below make call-site intent
	// clearer; prefer using them in new code.

	// Swap between host byte order and big-endian (network) byte order.
	// Self-inverse: host_to_be16(be_to_host16(x)) == x.
	static uint16_t to_endian_u16(uint16_t in) {
		return ntohs(in);
	}
	static uint16_t host_to_be16(uint16_t in) {
		return to_endian_u16(in);
	}
	static uint16_t be_to_host16(uint16_t in) {
		return to_endian_u16(in);
	}

	static uint32_t to_endian_u32(uint32_t in) {
		return ntohl(in);
	}
	static uint32_t host_to_be32(uint32_t in) {
		return to_endian_u32(in);
	}
	static uint32_t be_to_host32(uint32_t in) {
		return to_endian_u32(in);
	}

	static uint64_t to_endian_u64(uint64_t in) {
		uint64_t ret = 0;
		uint8_t* p = (uint8_t*)&in;
		ret = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) | ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
		      ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) | ((uint64_t)p[6] << 8) | ((uint64_t)p[7]);
		return ret;
	}
	static uint64_t host_to_be64(uint64_t in) {
		return to_endian_u64(in);
	}
	static uint64_t be_to_host64(uint64_t in) {
		return to_endian_u64(in);
	}

	// Encode signed int64 to sortable format (flip sign bit for proper ordering)
	static uint64_t encode_i64(int64_t val) {
		uint64_t uval = static_cast<uint64_t>(val);
		return uval ^ (1ULL << 63); // Flip sign bit for proper sorting
	}

	// Encode unsigned int64
	static uint64_t encode_u64(uint64_t val) {
		return val;
	}

	// Decode signed int64 from sortable format
	static int64_t decode_i64(uint64_t encoded) {
		return static_cast<int64_t>(encoded ^ (1ULL << 63));
	}

	// Decode from rocksdb::Slice
	static int64_t decode_i64(const rocksdb::Slice& key) {
		if (key.size() < 8)
			return 0;
		uint64_t val;
		memcpy(&val, key.data(), sizeof(val));
		return decode_i64(to_endian_u64(val));
	}

	static uint64_t decode_u64(const rocksdb::Slice& key) {
		if (key.size() < 8)
			return 0;
		uint64_t val;
		memcpy(&val, key.data(), sizeof(val));
		return to_endian_u64(val);
	}

	// Append methods for building keys
	static void append_i64(std::string& key, int64_t val) {
		uint64_t encoded = to_endian_u64(encode_i64(val));
		key.append((char*)&encoded, sizeof(encoded));
	}

	static void append_u64(std::string& key, uint64_t val) {
		uint64_t be = to_endian_u64(val);
		key.append((char*)&be, sizeof(be));
	}

	static void append_u32(std::string& key, uint32_t val) {
		uint32_t be = htonl(val);
		key.append((char*)&be, sizeof(be));
	}

	static void append_u16(std::string& key, uint16_t val) {
		uint16_t be = htons(val);
		key.append((char*)&be, sizeof(be));
	}

	static void append_u8(std::string& key, uint8_t val) {
		key.push_back((char)val);
	}
};

} // namespace neokv
