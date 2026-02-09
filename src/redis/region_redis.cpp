// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "region.h"
#include "closure.h"

#include "redis_codec.h"
#include "redis_common.h"
#include "redis_metadata.h"
#include "redis_service.h"
#include "key_encoder.h"

#include <random>
#include <unordered_set>
#include <cmath>
#include <climits>

namespace neokv {

// Helper: encode a uint64 index as an 8-byte big-endian string (for list subkeys)
static std::string encode_list_index(uint64_t index) {
	uint64_t be = KeyEncoder::host_to_be64(index);
	return std::string(reinterpret_cast<const char*>(&be), 8);
}

// Helper: decode a uint64 index from an 8-byte big-endian string
static uint64_t decode_list_index(const std::string& encoded) {
	if (encoded.size() != 8)
		return 0;
	uint64_t be;
	std::memcpy(&be, encoded.data(), 8);
	return KeyEncoder::be_to_host64(be);
}

// Helper: read list metadata from metadata CF
// Returns: 0 = found (not expired/dead), 1 = not found/expired/dead, -1 = error
static int read_list_metadata(RocksWrapper* rocks, int64_t region_id, int64_t index_id, uint16_t slot,
                              const std::string& user_key, RedisListMetadata* list_meta) {
	rocksdb::ReadOptions read_options;
	std::string meta_key = RedisMetadataKey::encode(region_id, index_id, slot, user_key);
	std::string raw_value;
	auto status = rocks->get(read_options, rocks->get_redis_metadata_handle(), meta_key, &raw_value);

	if (status.IsNotFound())
		return 1;
	if (!status.ok())
		return -1;

	RedisListMetadata m;
	if (!m.decode(raw_value))
		return -1;
	if (m.is_dead())
		return 1;
	if (m.type() != kRedisList)
		return 1; // wrong type treated as not found for list operations
	*list_meta = m;
	return 0;
}

// Helper: encode a String metadata value (flags + expire + inline_value)
static std::string encode_string_metadata_value(int64_t expire_ms, const std::string& payload) {
	RedisMetadata meta(kRedisString, false);
	meta.expire = (expire_ms > 0) ? static_cast<uint64_t>(expire_ms) : 0;
	std::string result;
	meta.encode(&result);
	result.append(payload);
	return result;
}

// Helper: read a String key from metadata CF, with fallback to data CF.
// Returns: 0 = found (not expired), 1 = not found or expired, -1 = error
static int read_string_key(RocksWrapper* rocks, int64_t region_id, int64_t index_id, uint16_t slot,
                           const std::string& user_key, std::string* payload, int64_t* expire_ms) {
	rocksdb::ReadOptions read_options;

	std::string meta_key = RedisMetadataKey::encode(region_id, index_id, slot, user_key);
	std::string raw_value;
	auto status = rocks->get(read_options, rocks->get_redis_metadata_handle(), meta_key, &raw_value);

	if (status.ok()) {
		RedisMetadata meta(kRedisNone, false);
		const char* ptr = raw_value.data();
		size_t remaining = raw_value.size();
		if (!meta.decode(&ptr, &remaining))
			return -1;
		if (meta.type() != kRedisString)
			return 1;
		if (meta.is_expired())
			return 1;
		if (payload)
			payload->assign(ptr, remaining);
		if (expire_ms)
			*expire_ms = (meta.expire > 0) ? static_cast<int64_t>(meta.expire) : 0;
		return 0;
	}
	if (!status.IsNotFound())
		return -1;

	// Fallback: old data CF
	std::string old_key = RedisCodec::encode_key(region_id, index_id, slot, user_key, REDIS_STRING);
	status = rocks->get(read_options, rocks->get_data_handle(), old_key, &raw_value);
	if (status.IsNotFound())
		return 1;
	if (!status.ok())
		return -1;

	int64_t old_expire = 0;
	std::string old_payload;
	if (!RedisCodec::decode_value(raw_value, &old_expire, &old_payload))
		return -1;
	if (RedisCodec::is_expired(old_expire, RedisCodec::current_time_ms()))
		return 1;
	if (payload)
		*payload = std::move(old_payload);
	if (expire_ms)
		*expire_ms = old_expire;
	return 0;
}

// Helper: read metadata for any key type from metadata CF.
// Returns: 0 = found (not expired/dead), 1 = not found/expired/dead, -1 = error
// On success, *meta is populated.
static int read_metadata(RocksWrapper* rocks, int64_t region_id, int64_t index_id, uint16_t slot,
                         const std::string& user_key, RedisMetadata* meta) {
	rocksdb::ReadOptions read_options;
	std::string meta_key = RedisMetadataKey::encode(region_id, index_id, slot, user_key);
	std::string raw_value;
	auto status = rocks->get(read_options, rocks->get_redis_metadata_handle(), meta_key, &raw_value);

	if (status.IsNotFound())
		return 1;
	if (!status.ok())
		return -1;

	RedisMetadata m(kRedisNone, false);
	const char* ptr = raw_value.data();
	size_t remaining = raw_value.size();
	if (!m.decode(&ptr, &remaining))
		return -1;
	if (m.is_dead())
		return 1;
	*meta = m;
	return 0;
}

void Region::apply_redis_write(const pb::StoreReq& request, braft::Closure* done, int64_t index, int64_t term) {
	TimeCost cost;

	if (!request.has_redis_req()) {
		DB_WARNING("redis_req not set, region_id: %ld", _region_id);
		if (done != nullptr) {
			((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
			((DMLClosure*)done)->response->set_errmsg("redis_req not set");
			((DMLClosure*)done)->applied_index = _applied_index;
		}
		return;
	}

	const pb::RedisWriteRequest& redis_req = request.redis_req();
	pb::RedisCmd cmd = redis_req.cmd();
	const pb::RedisSetCondition set_condition =
	    redis_req.has_set_condition() ? redis_req.set_condition() : pb::REDIS_SET_CONDITION_NONE;
	if (cmd == pb::REDIS_SET && set_condition != pb::REDIS_SET_CONDITION_NONE &&
	    set_condition != pb::REDIS_SET_CONDITION_NX && set_condition != pb::REDIS_SET_CONDITION_XX) {
		DB_WARNING("invalid redis set_condition: %d, region_id: %ld", set_condition, _region_id);
		if (done != nullptr) {
			((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
			((DMLClosure*)done)->response->set_errmsg("invalid redis set condition");
			((DMLClosure*)done)->applied_index = _applied_index;
		}
		return;
	}

	auto* rocks = RocksWrapper::get_instance();
	if (rocks == nullptr) {
		DB_FATAL("RocksWrapper is null, region_id: %ld", _region_id);
		if (done != nullptr) {
			((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
			((DMLClosure*)done)->response->set_errmsg("RocksWrapper is null");
			((DMLClosure*)done)->applied_index = _applied_index;
		}
		return;
	}

	rocksdb::WriteBatch batch;
	int64_t num_affected = 0;

	// In neo-redis, table_id is the authoritative Redis index_id.
	int64_t index_id = get_table_id();
	if (index_id <= 0) {
		DB_FATAL("invalid redis index_id(table_id): %ld, region_id: %ld", index_id, _region_id);
		if (done != nullptr) {
			((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
			((DMLClosure*)done)->response->set_errmsg("invalid redis index_id");
			((DMLClosure*)done)->applied_index = _applied_index;
		}
		return;
	}

	// Handle FLUSHDB: delete all keys in this region using DeleteRange.
	if (cmd == pb::REDIS_FLUSHDB) {
		std::string region_prefix = RedisCodec::build_region_prefix(_region_id, index_id);
		std::string region_end = region_prefix;
		bool carry = true;
		for (int i = static_cast<int>(region_end.size()) - 1; i >= 0 && carry; --i) {
			unsigned char c = static_cast<unsigned char>(region_end[i]);
			if (c < 0xFF) {
				region_end[i] = static_cast<char>(c + 1);
				carry = false;
			} else {
				region_end[i] = '\0';
			}
		}

		if (!carry) {
			rocksdb::TransactionDBWriteOptimizations opt;
			opt.skip_concurrency_control = true;
			opt.skip_duplicate_key_check = true;
			rocksdb::WriteBatch flush_batch;
			// Flush data CF (old String keys + future subkeys)
			flush_batch.DeleteRange(rocks->get_data_handle(), region_prefix, region_end);
			// Flush metadata CF
			flush_batch.DeleteRange(rocks->get_redis_metadata_handle(), region_prefix, region_end);
			// Flush zset score CF
			flush_batch.DeleteRange(rocks->get_redis_zset_score_handle(), region_prefix, region_end);
			rocksdb::WriteOptions write_options;
			auto status = rocks->get_db()->Write(write_options, opt, &flush_batch);
			if (!status.ok()) {
				DB_FATAL("Redis FLUSHDB failed: %s, region_id: %ld", status.ToString().c_str(), _region_id);
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg(status.ToString());
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}
		}

		_meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
		if (done != nullptr) {
			((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
			((DMLClosure*)done)->response->set_affected_rows(0);
			((DMLClosure*)done)->applied_index = _applied_index;
		}
		DB_WARNING("Redis FLUSHDB done, region_id=%ld, cost=%ld", _region_id, cost.get_time());
		return;
	}

	for (const auto& kv : redis_req.kvs()) {
		const std::string& user_key = kv.key();
		uint16_t slot = redis_slot(user_key);

		// Metadata CF key (no type_tag suffix)
		std::string meta_key = RedisMetadataKey::encode(_region_id, index_id, slot, user_key);

		switch (cmd) {
		case pb::REDIS_SET:
		case pb::REDIS_MSET: {
			if (cmd == pb::REDIS_SET && set_condition != pb::REDIS_SET_CONDITION_NONE) {
				// Check if key exists (metadata CF + fallback to data CF)
				int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, nullptr, nullptr);
				bool key_exists = (ret == 0);

				if (ret == -1) {
					DB_WARNING("redis pre-read failed, region_id: %ld", _region_id);
					if (done != nullptr) {
						((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
						((DMLClosure*)done)->response->set_errmsg("redis pre-read failed");
						((DMLClosure*)done)->applied_index = _applied_index;
					}
					return;
				}

				if ((set_condition == pb::REDIS_SET_CONDITION_NX && key_exists) ||
				    (set_condition == pb::REDIS_SET_CONDITION_XX && !key_exists)) {
					break;
				}
			}
			int64_t expire_ms = kv.has_expire_ms() ? kv.expire_ms() : REDIS_NO_EXPIRE;
			std::string meta_value = encode_string_metadata_value(expire_ms, kv.value());
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			// Also delete old data CF key if it exists (migration cleanup)
			std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
			batch.Delete(rocks->get_data_handle(), old_key);
			++num_affected;
			break;
		}
		case pb::REDIS_DEL: {
			// Check if key exists in metadata CF (any type)
			RedisMetadata del_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &del_meta);
			if (ret == 0) {
				// Delete from metadata CF (subkeys cleaned by compaction filter)
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				++num_affected;
			} else {
				// Fallback: check old data CF for legacy string keys
				ret = read_string_key(rocks, _region_id, index_id, slot, user_key, nullptr, nullptr);
				if (ret == 0) {
					batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
					std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
					batch.Delete(rocks->get_data_handle(), old_key);
					++num_affected;
				}
			}
			break;
		}
		case pb::REDIS_EXPIRE:
		case pb::REDIS_EXPIREAT: {
			// Read existing value, update expire time
			std::string payload;
			int64_t old_expire = 0;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);
			if (ret == 0) {
				int64_t new_expire = kv.expire_ms();
				std::string meta_value = encode_string_metadata_value(new_expire, payload);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
				// Clean up old data CF key
				std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
				batch.Delete(rocks->get_data_handle(), old_key);
				++num_affected;
			}
			break;
		}
		case pb::REDIS_PERSIST: {
			// Remove TTL
			std::string payload;
			int64_t old_expire = 0;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);
			if (ret == 0 && old_expire != REDIS_NO_EXPIRE) {
				std::string meta_value = encode_string_metadata_value(REDIS_NO_EXPIRE, payload);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
				std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
				batch.Delete(rocks->get_data_handle(), old_key);
				++num_affected;
			}
			break;
		}
		case pb::REDIS_HSET: {
			// HSET key field value [field value ...]
			// Read or create metadata
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret == -1) {
				DB_WARNING("redis HSET read_metadata failed, region_id: %ld", _region_id);
				break;
			}

			uint64_t version;
			uint64_t current_size;
			bool is_new = (ret == 1 || existing_meta.type() != kRedisHash);

			if (is_new) {
				// Create new hash metadata
				RedisMetadata new_meta(kRedisHash, true);
				version = new_meta.version;
				current_size = 0;
				// If overwriting a different type, delete old metadata
				if (ret == 0 && existing_meta.type() != kRedisHash) {
					// Type mismatch — overwrite (Redis HSET on a non-hash key creates a new hash)
					// Old subkeys will be cleaned by compaction filter (version mismatch)
				}
			} else {
				version = existing_meta.version;
				current_size = existing_meta.size;
			}

			// Process field-value pairs from the fields repeated field
			int64_t new_fields = 0;
			for (const auto& fv : redis_req.fields()) {
				const std::string& field = fv.field();
				const std::string& value = fv.value();

				// Check if field already exists
				std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, field);
				if (!is_new) {
					rocksdb::ReadOptions ro;
					std::string existing_val;
					auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &existing_val);
					if (s.IsNotFound()) {
						++new_fields;
					}
					// If field exists, we overwrite it (no size change)
				} else {
					++new_fields;
				}

				batch.Put(rocks->get_data_handle(), subkey, value);
			}

			// Update metadata with new size
			current_size += new_fields;
			RedisMetadata updated_meta(kRedisHash, false);
			updated_meta.version = version;
			updated_meta.size = current_size;
			updated_meta.expire = is_new ? 0 : existing_meta.expire;
			std::string meta_value;
			updated_meta.encode(&meta_value);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);

			num_affected = new_fields;
			break;
		}
		case pb::REDIS_HDEL: {
			// HDEL key field [field ...]
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret != 0 || existing_meta.type() != kRedisHash) {
				// Key doesn't exist or wrong type
				break;
			}

			uint64_t version = existing_meta.version;
			uint64_t current_size = existing_meta.size;
			int64_t deleted = 0;

			for (const auto& fv : redis_req.fields()) {
				const std::string& field = fv.field();
				std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, field);

				rocksdb::ReadOptions ro;
				std::string existing_val;
				auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &existing_val);
				if (s.ok()) {
					batch.Delete(rocks->get_data_handle(), subkey);
					++deleted;
				}
			}

			if (deleted > 0) {
				current_size = (current_size > static_cast<uint64_t>(deleted)) ? (current_size - deleted) : 0;
				if (current_size == 0) {
					// Hash is now empty — delete metadata
					batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				} else {
					RedisMetadata updated_meta(kRedisHash, false);
					updated_meta.version = version;
					updated_meta.size = current_size;
					updated_meta.expire = existing_meta.expire;
					std::string meta_value;
					updated_meta.encode(&meta_value);
					batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
				}
			}

			num_affected = deleted;
			break;
		}
		case pb::REDIS_HSETNX: {
			// HSETNX key field value — set field only if it doesn't exist
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret == -1)
				break;

			uint64_t version;
			uint64_t current_size;
			bool is_new = (ret == 1 || existing_meta.type() != kRedisHash);

			if (is_new) {
				RedisMetadata new_meta(kRedisHash, true);
				version = new_meta.version;
				current_size = 0;
			} else {
				version = existing_meta.version;
				current_size = existing_meta.size;
			}

			if (redis_req.fields_size() > 0) {
				const std::string& field = redis_req.fields(0).field();
				const std::string& value = redis_req.fields(0).value();

				std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, field);

				bool field_exists = false;
				if (!is_new) {
					rocksdb::ReadOptions ro;
					std::string existing_val;
					auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &existing_val);
					field_exists = s.ok();
				}

				if (!field_exists) {
					batch.Put(rocks->get_data_handle(), subkey, value);
					++current_size;
					num_affected = 1;

					RedisMetadata updated_meta(kRedisHash, false);
					updated_meta.version = version;
					updated_meta.size = current_size;
					updated_meta.expire = is_new ? 0 : existing_meta.expire;
					std::string meta_value;
					updated_meta.encode(&meta_value);
					batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
				}
			}
			break;
		}
		case pb::REDIS_HINCRBY: {
			// HINCRBY key field increment
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret == -1)
				break;

			uint64_t version;
			uint64_t current_size;
			bool is_new = (ret == 1 || existing_meta.type() != kRedisHash);

			if (is_new) {
				RedisMetadata new_meta(kRedisHash, true);
				version = new_meta.version;
				current_size = 0;
			} else {
				version = existing_meta.version;
				current_size = existing_meta.size;
			}

			if (redis_req.fields_size() > 0) {
				const std::string& field = redis_req.fields(0).field();
				const std::string& incr_str = redis_req.fields(0).value();

				int64_t increment = 0;
				try {
					increment = std::stoll(incr_str);
				} catch (...) {
					break;
				}

				std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, field);

				int64_t current_val = 0;
				bool field_exists = false;
				if (!is_new) {
					rocksdb::ReadOptions ro;
					std::string existing_val;
					auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &existing_val);
					if (s.ok()) {
						field_exists = true;
						try {
							current_val = std::stoll(existing_val);
						} catch (...) {
							// Not an integer — return error
							if (done != nullptr) {
								((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
								((DMLClosure*)done)->response->set_errmsg("ERR hash value is not an integer");
								((DMLClosure*)done)->applied_index = _applied_index;
							}
							return;
						}
					}
				}

				int64_t new_val = current_val + increment;
				std::string new_val_str = std::to_string(new_val);
				batch.Put(rocks->get_data_handle(), subkey, new_val_str);

				if (!field_exists) {
					++current_size;
				}

				RedisMetadata updated_meta(kRedisHash, false);
				updated_meta.version = version;
				updated_meta.size = current_size;
				updated_meta.expire = is_new ? 0 : existing_meta.expire;
				std::string meta_value;
				updated_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);

				// Store the result value in the response for the handler to read
				num_affected = new_val;
			}
			break;
		}
		case pb::REDIS_HINCRBYFLOAT: {
			// HINCRBYFLOAT key field increment
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret == -1)
				break;

			uint64_t version;
			uint64_t current_size;
			bool is_new = (ret == 1 || existing_meta.type() != kRedisHash);

			if (is_new) {
				RedisMetadata new_meta(kRedisHash, true);
				version = new_meta.version;
				current_size = 0;
			} else {
				version = existing_meta.version;
				current_size = existing_meta.size;
			}

			if (redis_req.fields_size() > 0) {
				const std::string& field = redis_req.fields(0).field();
				const std::string& incr_str = redis_req.fields(0).value();

				double increment = 0;
				try {
					increment = std::stod(incr_str);
				} catch (...) {
					break;
				}

				std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, field);

				double current_val = 0;
				bool field_exists = false;
				if (!is_new) {
					rocksdb::ReadOptions ro;
					std::string existing_val;
					auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &existing_val);
					if (s.ok()) {
						field_exists = true;
						try {
							current_val = std::stod(existing_val);
						} catch (...) {
							// Not a valid float — return error
							if (done != nullptr) {
								((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
								((DMLClosure*)done)->response->set_errmsg("ERR hash value is not a valid float");
								((DMLClosure*)done)->applied_index = _applied_index;
							}
							return;
						}
					}
				}

				double new_val = current_val + increment;
				// Format like Redis: remove trailing zeros
				char buf[256];
				snprintf(buf, sizeof(buf), "%.17g", new_val);
				std::string new_val_str(buf);
				batch.Put(rocks->get_data_handle(), subkey, new_val_str);

				if (!field_exists) {
					++current_size;
				}

				RedisMetadata updated_meta(kRedisHash, false);
				updated_meta.version = version;
				updated_meta.size = current_size;
				updated_meta.expire = is_new ? 0 : existing_meta.expire;
				std::string meta_value;
				updated_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);

				// Store result in response errmsg field (hack for float result)
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errmsg(new_val_str);
				}
				num_affected = 1;
			}
			break;
		}
		case pb::REDIS_SADD: {
			// SADD key member [member ...]
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret == -1)
				break;

			uint64_t version;
			uint64_t current_size;
			bool is_new = (ret == 1 || existing_meta.type() != kRedisSet);

			if (is_new) {
				RedisMetadata new_meta(kRedisSet, true);
				version = new_meta.version;
				current_size = 0;
			} else {
				version = existing_meta.version;
				current_size = existing_meta.size;
			}

			// Deduplicate input members
			std::unordered_set<std::string> seen;
			uint64_t added = 0;
			for (int i = 0; i < redis_req.members_size(); ++i) {
				const std::string& member = redis_req.members(i);
				if (!seen.insert(member).second)
					continue; // duplicate in input

				std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, member);

				// Check if member already exists
				if (!is_new) {
					rocksdb::ReadOptions ro;
					std::string existing_val;
					auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &existing_val);
					if (s.ok())
						continue; // already exists
				}

				// Add member — value is empty for sets
				batch.Put(rocks->get_data_handle(), subkey, rocksdb::Slice());
				++added;
			}

			if (added > 0) {
				current_size += added;
				RedisMetadata updated_meta(kRedisSet, false);
				updated_meta.version = version;
				updated_meta.size = current_size;
				updated_meta.expire = is_new ? 0 : existing_meta.expire;
				std::string meta_value;
				updated_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			} else if (is_new && redis_req.members_size() > 0) {
				// All members were duplicates but key is new — still need to create metadata
				// Actually no — if added == 0 and is_new, nothing to create
			}

			num_affected = added;
			break;
		}
		case pb::REDIS_SREM: {
			// SREM key member [member ...]
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret != 0 || existing_meta.type() != kRedisSet) {
				num_affected = 0;
				break;
			}

			uint64_t version = existing_meta.version;
			uint64_t current_size = existing_meta.size;

			std::unordered_set<std::string> seen;
			uint64_t removed = 0;
			for (int i = 0; i < redis_req.members_size(); ++i) {
				const std::string& member = redis_req.members(i);
				if (!seen.insert(member).second)
					continue;

				std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, member);
				rocksdb::ReadOptions ro;
				std::string existing_val;
				auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &existing_val);
				if (s.ok()) {
					batch.Delete(rocks->get_data_handle(), subkey);
					++removed;
				}
			}

			if (removed > 0) {
				if (current_size <= removed) {
					// All members removed — delete metadata
					batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				} else {
					current_size -= removed;
					RedisMetadata updated_meta(kRedisSet, false);
					updated_meta.version = version;
					updated_meta.size = current_size;
					updated_meta.expire = existing_meta.expire;
					std::string meta_value;
					updated_meta.encode(&meta_value);
					batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
				}
			}

			num_affected = removed;
			break;
		}
		case pb::REDIS_SPOP: {
			// SPOP key [count]
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret != 0 || existing_meta.type() != kRedisSet) {
				// Return empty — store popped members in errmsg as newline-separated
				num_affected = 0;
				break;
			}

			uint64_t version = existing_meta.version;
			uint64_t current_size = existing_meta.size;

			// Determine count from kvs[0].expire_ms (repurposed field)
			int64_t count = 1;
			if (redis_req.kvs_size() > 0 && redis_req.kvs(0).expire_ms() != 0) {
				count = redis_req.kvs(0).expire_ms();
			}
			if (count <= 0) {
				num_affected = 0;
				break;
			}

			// Load all members
			std::string prefix = RedisSubkeyKey::encode_prefix(_region_id, index_id, slot, user_key, version);
			rocksdb::ReadOptions ro;
			ro.prefix_same_as_start = true;
			std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(ro, rocks->get_data_handle()));

			std::vector<std::string> all_members;
			std::vector<std::string> all_subkeys;
			for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
				rocksdb::Slice key = iter->key();
				if (!key.starts_with(prefix))
					break;
				all_subkeys.emplace_back(key.data(), key.size());
				all_members.emplace_back(key.data() + prefix.size(), key.size() - prefix.size());
			}

			if (all_members.empty()) {
				num_affected = 0;
				break;
			}

			// Random selection (Fisher-Yates partial shuffle)
			size_t pop_count = std::min(static_cast<size_t>(count), all_members.size());
			std::mt19937 rng(std::random_device {}());
			for (size_t i = 0; i < pop_count; ++i) {
				std::uniform_int_distribution<size_t> dist(i, all_members.size() - 1);
				size_t j = dist(rng);
				if (i != j) {
					std::swap(all_members[i], all_members[j]);
					std::swap(all_subkeys[i], all_subkeys[j]);
				}
			}

			// Delete selected members and build result
			std::string popped_result;
			for (size_t i = 0; i < pop_count; ++i) {
				batch.Delete(rocks->get_data_handle(), all_subkeys[i]);
				if (i > 0)
					popped_result += '\n';
				popped_result += all_members[i];
			}

			// Update or delete metadata
			if (current_size <= pop_count) {
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
			} else {
				RedisMetadata updated_meta(kRedisSet, false);
				updated_meta.version = version;
				updated_meta.size = current_size - pop_count;
				updated_meta.expire = existing_meta.expire;
				std::string meta_value;
				updated_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			}

			// Store popped members in errmsg for handler to parse
			if (done != nullptr) {
				((DMLClosure*)done)->response->set_errmsg(popped_result);
			}
			num_affected = pop_count;
			break;
		}
		case pb::REDIS_SMOVE: {
			// SMOVE source destination member
			// kvs[0].key = source, kvs[0].value = destination, members[0] = member
			if (redis_req.kvs_size() < 1 || redis_req.members_size() < 1) {
				num_affected = 0;
				break;
			}
			std::string source_key = redis_req.kvs(0).key();
			std::string dest_key = redis_req.kvs(0).value();
			std::string member = redis_req.members(0);

			// Check source set exists and contains the member
			RedisMetadata src_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, source_key, &src_meta);
			if (ret != 0 || src_meta.type() != kRedisSet) {
				num_affected = 0;
				break;
			}

			std::string src_subkey =
			    RedisSubkeyKey::encode(_region_id, index_id, slot, source_key, src_meta.version, member);
			rocksdb::ReadOptions ro;
			std::string dummy;
			auto s = rocks->get(ro, rocks->get_data_handle(), src_subkey, &dummy);
			if (!s.ok()) {
				// Member not in source
				num_affected = 0;
				break;
			}

			// Remove from source
			batch.Delete(rocks->get_data_handle(), src_subkey);
			if (src_meta.size <= 1) {
				std::string src_meta_key = RedisMetadataKey::encode(_region_id, index_id, slot, source_key);
				batch.Delete(rocks->get_redis_metadata_handle(), src_meta_key);
			} else {
				RedisMetadata updated_src(kRedisSet, false);
				updated_src.version = src_meta.version;
				updated_src.size = src_meta.size - 1;
				updated_src.expire = src_meta.expire;
				std::string src_meta_value;
				updated_src.encode(&src_meta_value);
				std::string src_meta_key = RedisMetadataKey::encode(_region_id, index_id, slot, source_key);
				batch.Put(rocks->get_redis_metadata_handle(), src_meta_key, src_meta_value);
			}

			// Add to destination
			RedisMetadata dst_meta(kRedisNone, false);
			ret = read_metadata(rocks, _region_id, index_id, slot, dest_key, &dst_meta);
			bool dst_is_new = (ret != 0 || dst_meta.type() != kRedisSet);

			uint64_t dst_version;
			uint64_t dst_size;
			int64_t dst_expire;
			if (dst_is_new) {
				RedisMetadata new_meta(kRedisSet, true);
				dst_version = new_meta.version;
				dst_size = 0;
				dst_expire = 0;
			} else {
				dst_version = dst_meta.version;
				dst_size = dst_meta.size;
				dst_expire = dst_meta.expire;
			}

			std::string dst_subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, dest_key, dst_version, member);
			// Check if member already exists in destination
			std::string dst_dummy;
			auto ds = rocks->get(ro, rocks->get_data_handle(), dst_subkey, &dst_dummy);
			bool already_in_dst = ds.ok();

			batch.Put(rocks->get_data_handle(), dst_subkey, rocksdb::Slice());
			if (!already_in_dst) {
				dst_size += 1;
			}

			RedisMetadata updated_dst(kRedisSet, false);
			updated_dst.version = dst_version;
			updated_dst.size = dst_size;
			updated_dst.expire = dst_expire;
			std::string dst_meta_value;
			updated_dst.encode(&dst_meta_value);
			std::string dst_meta_key = RedisMetadataKey::encode(_region_id, index_id, slot, dest_key);
			batch.Put(rocks->get_redis_metadata_handle(), dst_meta_key, dst_meta_value);

			num_affected = 1;
			break;
		}
		case pb::REDIS_SINTERSTORE:
		case pb::REDIS_SUNIONSTORE:
		case pb::REDIS_SDIFFSTORE: {
			// *STORE destination members...
			// The handler computes the result set on the read side and passes
			// the final members list. The apply layer just replaces the destination
			// set with these members.
			// kvs[0].key = destination key, members = result members

			std::string dest_key = user_key; // already extracted from kvs[0].key

			// Delete old destination set if it exists
			RedisMetadata old_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, dest_key, &old_meta);
			if (ret == 0 && old_meta.type() == kRedisSet) {
				// Delete all old members
				std::string old_prefix =
				    RedisSubkeyKey::encode_prefix(_region_id, index_id, slot, dest_key, old_meta.version);
				rocksdb::ReadOptions ro;
				ro.prefix_same_as_start = true;
				std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(ro, rocks->get_data_handle()));
				for (iter->Seek(old_prefix); iter->Valid(); iter->Next()) {
					rocksdb::Slice key = iter->key();
					if (!key.starts_with(old_prefix))
						break;
					batch.Delete(rocks->get_data_handle(), key);
				}
			}

			if (redis_req.members_size() == 0) {
				// Result is empty — just delete the metadata
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				num_affected = 0;
			} else {
				// Create new set with the computed members
				RedisMetadata new_meta(kRedisSet, true);
				for (int i = 0; i < redis_req.members_size(); ++i) {
					std::string subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, dest_key, new_meta.version,
					                                            redis_req.members(i));
					batch.Put(rocks->get_data_handle(), subkey, rocksdb::Slice());
				}
				new_meta.size = redis_req.members_size();
				std::string meta_value;
				new_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
				num_affected = redis_req.members_size();
			}
			break;
		}
		case pb::REDIS_ZADD: {
			// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...
			// zset_entries has (score, member) pairs
			// flags in kvs[0].expire_ms: bit0=NX, bit1=XX, bit2=GT, bit3=LT, bit4=CH, bit5=INCR
			int64_t flags = (redis_req.kvs_size() > 0) ? redis_req.kvs(0).expire_ms() : 0;
			bool flag_nx = flags & 1, flag_xx = flags & 2, flag_gt = flags & 4, flag_lt = flags & 8;
			bool flag_ch = flags & 16, flag_incr = flags & 32;

			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			bool is_new = (ret != 0 || existing_meta.type() != kRedisZSet);
			uint64_t version;
			uint64_t current_size;
			int64_t old_expire;
			if (is_new) {
				RedisMetadata new_meta(kRedisZSet, true);
				version = new_meta.version;
				current_size = 0;
				old_expire = 0;
			} else {
				version = existing_meta.version;
				current_size = existing_meta.size;
				old_expire = existing_meta.expire;
			}

			uint64_t added = 0, changed = 0;
			std::string incr_result;
			for (int i = 0; i < redis_req.zset_entries_size(); ++i) {
				double score = redis_req.zset_entries(i).score();
				std::string member = redis_req.zset_entries(i).member();
				std::string data_subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, member);
				rocksdb::ReadOptions ro;
				std::string old_score_bytes;
				auto s = rocks->get(ro, rocks->get_data_handle(), data_subkey, &old_score_bytes);
				bool member_exists = s.ok() && old_score_bytes.size() >= 8;
				double old_score = member_exists ? RedisZSetScoreKey::decode_double(old_score_bytes.data()) : 0.0;

				if (flag_incr && member_exists)
					score = old_score + score;
				else if (flag_incr && !member_exists) { /* score stays as-is (increment from 0) */
				}

				if (member_exists && flag_nx) {
					if (flag_incr) {
						incr_result = "";
					} // NX + INCR on existing = nil
					continue;
				}
				if (!member_exists && flag_xx) {
					if (flag_incr) {
						incr_result = "";
					}
					continue;
				}
				if (member_exists && (flag_gt || flag_lt)) {
					bool should_update = (flag_gt && score > old_score) || (flag_lt && score < old_score);
					if (!should_update && !flag_incr)
						continue;
					if (!should_update && flag_incr) {
						score = old_score;
					}
				}

				if (member_exists && score == old_score) {
					if (flag_incr) {
						char buf[64];
						snprintf(buf, sizeof(buf), "%.17g", score);
						incr_result = buf;
					}
					continue; // No change needed
				}

				// Delete old score key if member existed
				if (member_exists) {
					std::string old_score_key =
					    RedisZSetScoreKey::encode(_region_id, index_id, slot, user_key, version, old_score, member);
					batch.Delete(rocks->get_redis_zset_score_handle(), old_score_key);
					++changed;
				} else {
					++added;
				}

				// Write new data subkey (member -> encoded score)
				std::string new_score_bytes;
				RedisZSetScoreKey::encode_double(score, &new_score_bytes);
				batch.Put(rocks->get_data_handle(), data_subkey, new_score_bytes);
				// Write new score key
				std::string new_score_key =
				    RedisZSetScoreKey::encode(_region_id, index_id, slot, user_key, version, score, member);
				batch.Put(rocks->get_redis_zset_score_handle(), new_score_key, rocksdb::Slice());

				if (flag_incr) {
					char buf[64];
					snprintf(buf, sizeof(buf), "%.17g", score);
					incr_result = buf;
				}
			}

			if (added > 0 || changed > 0) {
				current_size += added;
				RedisMetadata updated_meta(kRedisZSet, false);
				updated_meta.version = version;
				updated_meta.size = current_size;
				updated_meta.expire = old_expire;
				std::string meta_value;
				updated_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			}

			if (flag_incr) {
				if (done != nullptr && !incr_result.empty()) {
					((DMLClosure*)done)->response->set_errmsg(incr_result);
				}
				num_affected = incr_result.empty() ? 0 : 1;
			} else {
				num_affected = flag_ch ? (added + changed) : added;
			}
			break;
		}
		case pb::REDIS_ZREM: {
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret != 0 || existing_meta.type() != kRedisZSet) {
				num_affected = 0;
				break;
			}
			uint64_t version = existing_meta.version;
			uint64_t current_size = existing_meta.size;
			uint64_t removed = 0;
			for (int i = 0; i < redis_req.members_size(); ++i) {
				std::string member = redis_req.members(i);
				std::string data_subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, member);
				rocksdb::ReadOptions ro;
				std::string score_bytes;
				auto s = rocks->get(ro, rocks->get_data_handle(), data_subkey, &score_bytes);
				if (!s.ok() || score_bytes.size() < 8)
					continue;
				double score = RedisZSetScoreKey::decode_double(score_bytes.data());
				batch.Delete(rocks->get_data_handle(), data_subkey);
				std::string score_key =
				    RedisZSetScoreKey::encode(_region_id, index_id, slot, user_key, version, score, member);
				batch.Delete(rocks->get_redis_zset_score_handle(), score_key);
				++removed;
			}
			if (removed > 0) {
				if (current_size <= removed) {
					batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				} else {
					RedisMetadata updated(kRedisZSet, false);
					updated.version = version;
					updated.size = current_size - removed;
					updated.expire = existing_meta.expire;
					std::string mv;
					updated.encode(&mv);
					batch.Put(rocks->get_redis_metadata_handle(), meta_key, mv);
				}
			}
			num_affected = removed;
			break;
		}
		case pb::REDIS_ZINCRBY: {
			// zset_entries[0] has (increment, member)
			if (redis_req.zset_entries_size() < 1) {
				num_affected = 0;
				break;
			}
			double increment = redis_req.zset_entries(0).score();
			std::string member = redis_req.zset_entries(0).member();
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			bool is_new = (ret != 0 || existing_meta.type() != kRedisZSet);
			uint64_t version;
			uint64_t current_size;
			int64_t old_expire;
			if (is_new) {
				RedisMetadata nm(kRedisZSet, true);
				version = nm.version;
				current_size = 0;
				old_expire = 0;
			} else {
				version = existing_meta.version;
				current_size = existing_meta.size;
				old_expire = existing_meta.expire;
			}
			std::string data_subkey = RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, member);
			rocksdb::ReadOptions ro;
			std::string old_score_bytes;
			auto s = rocks->get(ro, rocks->get_data_handle(), data_subkey, &old_score_bytes);
			bool member_exists = s.ok() && old_score_bytes.size() >= 8;
			double old_score = member_exists ? RedisZSetScoreKey::decode_double(old_score_bytes.data()) : 0.0;
			double new_score = old_score + increment;
			if (member_exists) {
				std::string old_sk =
				    RedisZSetScoreKey::encode(_region_id, index_id, slot, user_key, version, old_score, member);
				batch.Delete(rocks->get_redis_zset_score_handle(), old_sk);
			} else {
				current_size += 1;
			}
			std::string nsb;
			RedisZSetScoreKey::encode_double(new_score, &nsb);
			batch.Put(rocks->get_data_handle(), data_subkey, nsb);
			std::string nsk =
			    RedisZSetScoreKey::encode(_region_id, index_id, slot, user_key, version, new_score, member);
			batch.Put(rocks->get_redis_zset_score_handle(), nsk, rocksdb::Slice());
			RedisMetadata um(kRedisZSet, false);
			um.version = version;
			um.size = current_size;
			um.expire = old_expire;
			std::string mv;
			um.encode(&mv);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, mv);
			char buf[64];
			snprintf(buf, sizeof(buf), "%.17g", new_score);
			if (done != nullptr)
				((DMLClosure*)done)->response->set_errmsg(std::string(buf));
			num_affected = 1;
			break;
		}
		case pb::REDIS_ZPOPMIN:
		case pb::REDIS_ZPOPMAX: {
			RedisMetadata existing_meta(kRedisNone, false);
			int ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &existing_meta);
			if (ret != 0 || existing_meta.type() != kRedisZSet) {
				num_affected = 0;
				break;
			}
			uint64_t version = existing_meta.version;
			uint64_t current_size = existing_meta.size;
			int64_t count =
			    (redis_req.kvs_size() > 0 && redis_req.kvs(0).expire_ms() > 0) ? redis_req.kvs(0).expire_ms() : 1;
			bool pop_max = (redis_req.cmd() == pb::REDIS_ZPOPMAX);
			// Load all entries from score CF
			std::string prefix = RedisZSetScoreKey::encode_prefix(_region_id, index_id, slot, user_key, version);
			rocksdb::ReadOptions ro;
			ro.prefix_same_as_start = true;
			std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(ro, rocks->get_redis_zset_score_handle()));
			std::vector<std::pair<double, std::string>> entries;
			for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
				rocksdb::Slice key = iter->key();
				if (!key.starts_with(prefix))
					break;
				double score = RedisZSetScoreKey::decode_double(key.data() + prefix.size());
				std::string member(key.data() + prefix.size() + 8, key.size() - prefix.size() - 8);
				entries.emplace_back(score, std::move(member));
			}
			if (entries.empty()) {
				num_affected = 0;
				break;
			}
			size_t pop_count = std::min(static_cast<size_t>(count), entries.size());
			std::string result;
			for (size_t i = 0; i < pop_count; ++i) {
				size_t idx = pop_max ? (entries.size() - 1 - i) : i;
				auto& e = entries[idx];
				if (!result.empty())
					result += '\n';
				result += e.second;
				result += '\n';
				char buf[64];
				snprintf(buf, sizeof(buf), "%.17g", e.first);
				result += buf;
				// Delete from both CFs
				std::string data_subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, version, e.second);
				batch.Delete(rocks->get_data_handle(), data_subkey);
				std::string score_key =
				    RedisZSetScoreKey::encode(_region_id, index_id, slot, user_key, version, e.first, e.second);
				batch.Delete(rocks->get_redis_zset_score_handle(), score_key);
			}
			if (current_size <= pop_count) {
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
			} else {
				RedisMetadata um(kRedisZSet, false);
				um.version = version;
				um.size = current_size - pop_count;
				um.expire = existing_meta.expire;
				std::string mv;
				um.encode(&mv);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, mv);
			}
			if (done != nullptr)
				((DMLClosure*)done)->response->set_errmsg(result);
			num_affected = pop_count;
			break;
		}
		case pb::REDIS_INCR: {
			// INCR/DECR/INCRBY/DECRBY — increment is in kv.value()
			std::string payload;
			int64_t old_expire = 0;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);
			if (ret == -1) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR internal error");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			int64_t current_val = 0;
			if (ret == 0) {
				// Key exists — parse as integer
				try {
					size_t pos = 0;
					current_val = std::stoll(payload, &pos);
					if (pos != payload.size()) {
						if (done != nullptr) {
							((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
							((DMLClosure*)done)->response->set_errmsg("ERR value is not an integer or out of range");
							((DMLClosure*)done)->applied_index = _applied_index;
						}
						return;
					}
				} catch (...) {
					if (done != nullptr) {
						((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
						((DMLClosure*)done)->response->set_errmsg("ERR value is not an integer or out of range");
						((DMLClosure*)done)->applied_index = _applied_index;
					}
					return;
				}
			}
			// ret == 1 means key doesn't exist, current_val stays 0

			int64_t increment = 0;
			try {
				increment = std::stoll(kv.value());
			} catch (...) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR value is not an integer or out of range");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			// Overflow check
			if ((increment > 0 && current_val > INT64_MAX - increment) ||
			    (increment < 0 && current_val < INT64_MIN - increment)) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR increment or decrement would overflow");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			int64_t new_val = current_val + increment;
			std::string new_payload = std::to_string(new_val);
			int64_t expire_to_use = (ret == 0) ? old_expire : REDIS_NO_EXPIRE;
			std::string meta_value = encode_string_metadata_value(expire_to_use, new_payload);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			// Clean up old data CF key
			std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
			batch.Delete(rocks->get_data_handle(), old_key);

			num_affected = new_val; // Return the new value via affected_rows
			break;
		}
		case pb::REDIS_INCRBYFLOAT_STR: {
			// INCRBYFLOAT — float increment is in kv.value()
			std::string payload;
			int64_t old_expire = 0;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);
			if (ret == -1) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR internal error");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			double current_val = 0.0;
			if (ret == 0) {
				try {
					size_t pos = 0;
					current_val = std::stod(payload, &pos);
					if (pos != payload.size()) {
						if (done != nullptr) {
							((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
							((DMLClosure*)done)->response->set_errmsg("ERR value is not a valid float");
							((DMLClosure*)done)->applied_index = _applied_index;
						}
						return;
					}
				} catch (...) {
					if (done != nullptr) {
						((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
						((DMLClosure*)done)->response->set_errmsg("ERR value is not a valid float");
						((DMLClosure*)done)->applied_index = _applied_index;
					}
					return;
				}
			}

			double increment = 0.0;
			try {
				increment = std::stod(kv.value());
			} catch (...) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR value is not a valid float");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			double new_val = current_val + increment;
			if (std::isnan(new_val) || std::isinf(new_val)) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR increment would produce NaN or Infinity");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			char buf[256];
			snprintf(buf, sizeof(buf), "%.17g", new_val);
			std::string new_payload(buf);
			int64_t expire_to_use = (ret == 0) ? old_expire : REDIS_NO_EXPIRE;
			std::string meta_value = encode_string_metadata_value(expire_to_use, new_payload);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
			batch.Delete(rocks->get_data_handle(), old_key);

			// Store result string in errmsg for handler to read
			if (done != nullptr) {
				((DMLClosure*)done)->response->set_errmsg(new_payload);
			}
			num_affected = 1;
			break;
		}
		case pb::REDIS_APPEND: {
			// APPEND key value — append to existing string
			std::string payload;
			int64_t old_expire = 0;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);
			if (ret == -1) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR internal error");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			if (ret == 1) {
				// Key doesn't exist — start with empty string
				payload.clear();
				old_expire = REDIS_NO_EXPIRE;
			}

			// APPEND semantics: append new bytes after the existing payload.
			payload.append(kv.value());

			std::string meta_value = encode_string_metadata_value(old_expire, payload);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
			batch.Delete(rocks->get_data_handle(), old_key);

			num_affected = static_cast<int64_t>(payload.size()); // Return new length
			break;
		}
		case pb::REDIS_GETSET: {
			// GETSET key value — get old value, set new value
			std::string old_payload;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &old_payload, nullptr);
			if (ret == -1) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR internal error");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			if (ret == 0) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errmsg(old_payload);
				}
				num_affected = 1;
			} else {
				num_affected = 0;
			}

			// GETSET always writes the new value and clears TTL.
			std::string meta_value = encode_string_metadata_value(REDIS_NO_EXPIRE, kv.value());
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			// Also delete old data CF key (migration cleanup).
			std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
			batch.Delete(rocks->get_data_handle(), old_key);
			break;
		}
		case pb::REDIS_GETDEL: {
			// GETDEL key — get value and delete
			std::string old_payload;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &old_payload, nullptr);
			if (ret == -1) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR internal error");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			if (ret == 0) {
				// Delete from metadata CF
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				// Also delete old data CF key
				std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
				batch.Delete(rocks->get_data_handle(), old_key);

				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errmsg(old_payload);
				}
				num_affected = 1;
			} else {
				num_affected = 0;
			}
			break;
		}
		case pb::REDIS_GETEX: {
			// GETEX key — get value and update expire
			// New expire is in kv.expire_ms()
			std::string payload;
			int64_t old_expire = 0;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);
			if (ret == -1) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR internal error");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			if (ret == 0) {
				int64_t new_expire = kv.has_expire_ms() ? kv.expire_ms() : old_expire;
				std::string meta_value = encode_string_metadata_value(new_expire, payload);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
				std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
				batch.Delete(rocks->get_data_handle(), old_key);

				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errmsg(payload);
				}
				num_affected = 1;
			} else {
				num_affected = 0;
			}
			break;
		}
		case pb::REDIS_SETRANGE: {
			// SETRANGE key offset value
			// offset is in kv.expire_ms() (repurposed), value is in kv.value()
			std::string payload;
			int64_t old_expire = 0;
			int ret = read_string_key(rocks, _region_id, index_id, slot, user_key, &payload, &old_expire);
			if (ret == -1) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR internal error");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			if (ret == 1) {
				// Key doesn't exist — start with empty string
				payload.clear();
				old_expire = REDIS_NO_EXPIRE;
			}

			int64_t offset = kv.has_expire_ms() ? kv.expire_ms() : 0;
			const std::string& replace_value = kv.value();
			size_t needed = static_cast<size_t>(offset) + replace_value.size();

			// Pad with zero bytes if needed
			if (needed > payload.size()) {
				payload.resize(needed, '\0');
			}

			// Overwrite at offset
			memcpy(&payload[offset], replace_value.data(), replace_value.size());

			std::string meta_value = encode_string_metadata_value(old_expire, payload);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			std::string old_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);
			batch.Delete(rocks->get_data_handle(), old_key);

			num_affected = static_cast<int64_t>(payload.size()); // Return new length
			break;
		}
		case pb::REDIS_LPUSH:
		case pb::REDIS_RPUSH: {
			// LPUSH/RPUSH key element [element ...]
			// Elements come from redis_req.list_elements()
			// First check if key exists as a different type
			RedisMetadata any_meta(kRedisNone, false);
			int any_ret = read_metadata(rocks, _region_id, index_id, slot, user_key, &any_meta);
			if (any_ret == 0 && any_meta.type() != kRedisList) {
				// WRONGTYPE error
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)
					    ->response->set_errmsg("WRONGTYPE Operation against a key holding the wrong kind of value");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			RedisListMetadata list_meta;
			int ret = read_list_metadata(rocks, _region_id, index_id, slot, user_key, &list_meta);
			if (ret == -1) {
				DB_WARNING("redis LPUSH/RPUSH read_list_metadata failed, region_id: %ld", _region_id);
				break;
			}

			bool is_new = (ret == 1);
			if (!is_new && list_meta.type() != kRedisList) {
				// WRONGTYPE error
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)
					    ->response->set_errmsg("WRONGTYPE Operation against a key holding the wrong kind of value");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			if (is_new) {
				// Create new list metadata
				RedisListMetadata new_meta;
				list_meta = new_meta;
				list_meta.size = 0;
			}

			bool is_lpush = (cmd == pb::REDIS_LPUSH);
			int count = redis_req.list_elements_size();

			for (int i = 0; i < count; ++i) {
				const std::string& element = redis_req.list_elements(i);
				uint64_t idx;
				if (is_lpush) {
					--list_meta.head;
					idx = list_meta.head;
				} else {
					idx = list_meta.tail;
					++list_meta.tail;
				}
				std::string index_key = encode_list_index(idx);
				std::string subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
				batch.Put(rocks->get_data_handle(), subkey, element);
			}

			list_meta.size += count;
			if (is_new) {
				list_meta.expire = 0;
			}
			std::string meta_value;
			list_meta.encode(&meta_value);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);

			num_affected = static_cast<int64_t>(list_meta.size);
			break;
		}
		case pb::REDIS_LPOP:
		case pb::REDIS_RPOP: {
			// LPOP/RPOP key [count]
			RedisListMetadata list_meta;
			int ret = read_list_metadata(rocks, _region_id, index_id, slot, user_key, &list_meta);
			if (ret != 0 || list_meta.type() != kRedisList) {
				num_affected = 0;
				break;
			}

			// count from kvs[0].expire_ms (repurposed)
			int64_t count = 1;
			if (redis_req.kvs_size() > 0 && redis_req.kvs(0).expire_ms() != 0) {
				count = redis_req.kvs(0).expire_ms();
			}
			if (count <= 0) {
				num_affected = 0;
				break;
			}

			bool is_lpop = (cmd == pb::REDIS_LPOP);
			uint64_t pop_count = std::min(static_cast<uint64_t>(count), list_meta.size);

			std::string popped_result;
			for (uint64_t i = 0; i < pop_count; ++i) {
				uint64_t idx;
				if (is_lpop) {
					idx = list_meta.head;
					++list_meta.head;
				} else {
					--list_meta.tail;
					idx = list_meta.tail;
				}
				std::string index_key = encode_list_index(idx);
				std::string subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);

				// Read the element value
				rocksdb::ReadOptions ro;
				std::string element_value;
				auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &element_value);
				if (s.ok()) {
					if (i > 0)
						popped_result += '\n';
					popped_result += element_value;
				}
				batch.Delete(rocks->get_data_handle(), subkey);
			}

			list_meta.size -= pop_count;
			if (list_meta.size == 0) {
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
			} else {
				std::string meta_value;
				list_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			}

			if (done != nullptr) {
				((DMLClosure*)done)->response->set_errmsg(popped_result);
			}
			num_affected = pop_count;
			break;
		}
		case pb::REDIS_LSET: {
			// LSET key index element
			// index is in kvs[0].expire_ms (repurposed), element is in kvs[0].value
			RedisListMetadata list_meta;
			int ret = read_list_metadata(rocks, _region_id, index_id, slot, user_key, &list_meta);
			if (ret != 0 || list_meta.type() != kRedisList) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR no such key");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			int64_t index_val = kv.has_expire_ms() ? kv.expire_ms() : 0;
			int64_t list_size = static_cast<int64_t>(list_meta.size);

			// Normalize negative index
			if (index_val < 0)
				index_val = list_size + index_val;
			if (index_val < 0 || index_val >= list_size) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR index out of range");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			uint64_t actual_idx = list_meta.head + static_cast<uint64_t>(index_val);
			std::string index_key = encode_list_index(actual_idx);
			std::string subkey =
			    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
			batch.Put(rocks->get_data_handle(), subkey, kv.value());

			num_affected = 1;
			break;
		}
		case pb::REDIS_LINSERT: {
			// LINSERT key BEFORE|AFTER pivot element
			// pivot is in fields[0].field, element is in fields[0].value
			// direction: kvs[0].expire_ms = 0 for BEFORE, 1 for AFTER
			RedisListMetadata list_meta;
			int ret = read_list_metadata(rocks, _region_id, index_id, slot, user_key, &list_meta);
			if (ret != 0 || list_meta.type() != kRedisList || list_meta.size == 0) {
				num_affected = 0;
				break;
			}

			if (redis_req.fields_size() == 0) {
				num_affected = 0;
				break;
			}

			const std::string& pivot = redis_req.fields(0).field();
			const std::string& element = redis_req.fields(0).value();
			bool insert_after = (kv.has_expire_ms() && kv.expire_ms() == 1);

			// Find the pivot by scanning all elements
			int64_t pivot_logical_index = -1;
			std::string prefix = RedisSubkeyKey::encode_prefix(_region_id, index_id, slot, user_key, list_meta.version);
			rocksdb::ReadOptions ro;
			ro.prefix_same_as_start = true;
			std::unique_ptr<rocksdb::Iterator> iter(rocks->new_iterator(ro, rocks->get_data_handle()));

			// We need to scan in order (head to tail)
			for (uint64_t i = 0; i < list_meta.size; ++i) {
				uint64_t idx = list_meta.head + i;
				std::string index_key = encode_list_index(idx);
				std::string subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
				std::string val;
				auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &val);
				if (s.ok() && val == pivot) {
					pivot_logical_index = static_cast<int64_t>(i);
					break;
				}
			}

			if (pivot_logical_index < 0) {
				// Pivot not found
				num_affected = -1;
				break;
			}

			// Insert by shifting elements
			// For BEFORE: insert at pivot_logical_index, shift pivot and everything after right
			// For AFTER: insert at pivot_logical_index+1, shift everything after right
			int64_t insert_pos = insert_after ? (pivot_logical_index + 1) : pivot_logical_index;

			// Read all elements from insert_pos to end, shift them right by 1
			// We do this by reading tail-1 down to insert_pos and writing each to pos+1
			for (int64_t i = static_cast<int64_t>(list_meta.size) - 1; i >= insert_pos; --i) {
				uint64_t old_idx = list_meta.head + static_cast<uint64_t>(i);
				uint64_t new_idx = old_idx + 1;
				std::string old_index_key = encode_list_index(old_idx);
				std::string old_subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, old_index_key);
				std::string val;
				auto s = rocks->get(ro, rocks->get_data_handle(), old_subkey, &val);
				if (s.ok()) {
					std::string new_index_key = encode_list_index(new_idx);
					std::string new_subkey =
					    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, new_index_key);
					batch.Put(rocks->get_data_handle(), new_subkey, val);
				}
			}

			// Write the new element at insert_pos
			uint64_t insert_idx = list_meta.head + static_cast<uint64_t>(insert_pos);
			std::string insert_index_key = encode_list_index(insert_idx);
			std::string insert_subkey =
			    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, insert_index_key);
			batch.Put(rocks->get_data_handle(), insert_subkey, element);

			++list_meta.tail;
			++list_meta.size;
			std::string meta_value;
			list_meta.encode(&meta_value);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);

			num_affected = static_cast<int64_t>(list_meta.size);
			break;
		}
		case pb::REDIS_LREM: {
			// LREM key count element
			// count is in kvs[0].expire_ms (repurposed as signed), element is in kvs[0].value
			RedisListMetadata list_meta;
			int ret = read_list_metadata(rocks, _region_id, index_id, slot, user_key, &list_meta);
			if (ret != 0 || list_meta.type() != kRedisList) {
				if (done != nullptr) {
					((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
					((DMLClosure*)done)->response->set_errmsg("ERR no such key");
					((DMLClosure*)done)->applied_index = _applied_index;
				}
				return;
			}

			int64_t rem_count = kv.has_expire_ms() ? kv.expire_ms() : 0;
			const std::string& element = kv.value();

			// Read all elements
			std::vector<std::pair<uint64_t, std::string>> elements; // (index, value)
			for (uint64_t i = 0; i < list_meta.size; ++i) {
				uint64_t idx = list_meta.head + i;
				std::string index_key = encode_list_index(idx);
				std::string subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
				rocksdb::ReadOptions ro;
				std::string val;
				auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &val);
				if (s.ok()) {
					elements.emplace_back(idx, std::move(val));
				}
			}

			// Determine which elements to remove
			std::vector<size_t> remove_indices;
			if (rem_count > 0) {
				// Remove first N matching from head
				for (size_t i = 0; i < elements.size() && static_cast<int64_t>(remove_indices.size()) < rem_count;
				     ++i) {
					if (elements[i].second == element) {
						remove_indices.push_back(i);
					}
				}
			} else if (rem_count < 0) {
				// Remove first N matching from tail
				int64_t abs_count = -rem_count;
				for (int64_t i = static_cast<int64_t>(elements.size()) - 1;
				     i >= 0 && static_cast<int64_t>(remove_indices.size()) < abs_count; --i) {
					if (elements[i].second == element) {
						remove_indices.push_back(static_cast<size_t>(i));
					}
				}
			} else {
				// Remove all matching
				for (size_t i = 0; i < elements.size(); ++i) {
					if (elements[i].second == element) {
						remove_indices.push_back(i);
					}
				}
			}

			if (remove_indices.empty()) {
				num_affected = 0;
				break;
			}

			// Mark removed indices
			std::unordered_set<size_t> removed_set(remove_indices.begin(), remove_indices.end());

			// Delete all old subkeys
			for (const auto& elem : elements) {
				std::string index_key = encode_list_index(elem.first);
				std::string subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
				batch.Delete(rocks->get_data_handle(), subkey);
			}

			// Rebuild: write remaining elements with new contiguous indices
			std::vector<std::string> remaining;
			for (size_t i = 0; i < elements.size(); ++i) {
				if (removed_set.find(i) == removed_set.end()) {
					remaining.push_back(elements[i].second);
				}
			}

			if (remaining.empty()) {
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
			} else {
				// Reset head/tail and rewrite
				uint64_t new_head = UINT64_MAX / 2;
				uint64_t new_tail = new_head + remaining.size();
				for (size_t i = 0; i < remaining.size(); ++i) {
					uint64_t idx = new_head + i;
					std::string index_key = encode_list_index(idx);
					std::string subkey =
					    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
					batch.Put(rocks->get_data_handle(), subkey, remaining[i]);
				}
				list_meta.head = new_head;
				list_meta.tail = new_tail;
				list_meta.size = remaining.size();
				std::string meta_value;
				list_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			}

			num_affected = static_cast<int64_t>(remove_indices.size());
			break;
		}
		case pb::REDIS_LTRIM: {
			// LTRIM key start stop
			// start is in kvs[0].expire_ms (repurposed), stop is passed via fields[0].field as string
			RedisListMetadata list_meta;
			int ret = read_list_metadata(rocks, _region_id, index_id, slot, user_key, &list_meta);
			if (ret != 0 || list_meta.type() != kRedisList) {
				// Key doesn't exist — no-op, return OK
				num_affected = 1;
				break;
			}

			int64_t start = kv.has_expire_ms() ? kv.expire_ms() : 0;
			int64_t stop = 0;
			if (redis_req.fields_size() > 0) {
				try {
					stop = std::stoll(redis_req.fields(0).field());
				} catch (...) {
					stop = -1;
				}
			}

			int64_t list_size = static_cast<int64_t>(list_meta.size);

			// Normalize negative indices
			if (start < 0)
				start = list_size + start;
			if (stop < 0)
				stop = list_size + stop;
			if (start < 0)
				start = 0;

			// If start > stop or start >= size, the result is an empty list
			if (start > stop || start >= list_size) {
				// Delete all elements
				for (uint64_t i = 0; i < list_meta.size; ++i) {
					uint64_t idx = list_meta.head + i;
					std::string index_key = encode_list_index(idx);
					std::string subkey =
					    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
					batch.Delete(rocks->get_data_handle(), subkey);
				}
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				num_affected = 1;
				break;
			}

			if (stop >= list_size)
				stop = list_size - 1;

			// Delete elements outside [start, stop]
			// Delete elements before start
			for (int64_t i = 0; i < start; ++i) {
				uint64_t idx = list_meta.head + static_cast<uint64_t>(i);
				std::string index_key = encode_list_index(idx);
				std::string subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
				batch.Delete(rocks->get_data_handle(), subkey);
			}
			// Delete elements after stop
			for (int64_t i = stop + 1; i < list_size; ++i) {
				uint64_t idx = list_meta.head + static_cast<uint64_t>(i);
				std::string index_key = encode_list_index(idx);
				std::string subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);
				batch.Delete(rocks->get_data_handle(), subkey);
			}

			// Update metadata
			list_meta.head = list_meta.head + static_cast<uint64_t>(start);
			list_meta.tail = list_meta.head + static_cast<uint64_t>(stop - start + 1);
			list_meta.size = static_cast<uint64_t>(stop - start + 1);
			std::string meta_value;
			list_meta.encode(&meta_value);
			batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);

			num_affected = 1;
			break;
		}
		case pb::REDIS_LMOVE: {
			// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
			// source key is in kvs[0].key, dest key is in kvs[1].key (if different)
			// or fields[0].field for dest key
			// src_dir: kvs[0].expire_ms bit 0 (0=LEFT, 1=RIGHT)
			// dst_dir: kvs[0].expire_ms bit 1 (0=LEFT, 2=RIGHT)
			// dest key is in fields[0].field
			RedisListMetadata src_meta;
			int ret = read_list_metadata(rocks, _region_id, index_id, slot, user_key, &src_meta);
			if (ret != 0 || src_meta.type() != kRedisList || src_meta.size == 0) {
				num_affected = 0;
				break;
			}

			int64_t dir_flags = kv.has_expire_ms() ? kv.expire_ms() : 0;
			bool src_left = (dir_flags & 1) == 0;
			bool dst_left = (dir_flags & 2) == 0;

			std::string dest_key;
			if (redis_req.fields_size() > 0) {
				dest_key = redis_req.fields(0).field();
			} else {
				dest_key = user_key; // same key
			}

			// Pop from source
			uint64_t pop_idx;
			if (src_left) {
				pop_idx = src_meta.head;
			} else {
				pop_idx = src_meta.tail - 1;
			}

			std::string pop_index_key = encode_list_index(pop_idx);
			std::string pop_subkey =
			    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, src_meta.version, pop_index_key);
			rocksdb::ReadOptions ro;
			std::string popped_value;
			auto s = rocks->get(ro, rocks->get_data_handle(), pop_subkey, &popped_value);
			if (!s.ok()) {
				num_affected = 0;
				break;
			}
			batch.Delete(rocks->get_data_handle(), pop_subkey);

			if (src_left) {
				++src_meta.head;
			} else {
				--src_meta.tail;
			}
			--src_meta.size;

			// Push to destination
			bool same_key = (dest_key == user_key);
			uint16_t dest_slot = redis_slot(dest_key);
			std::string dest_meta_key = RedisMetadataKey::encode(_region_id, index_id, dest_slot, dest_key);

			RedisListMetadata dst_meta;
			if (same_key) {
				dst_meta = src_meta;
			} else {
				ret = read_list_metadata(rocks, _region_id, index_id, dest_slot, dest_key, &dst_meta);
				if (ret == -1) {
					num_affected = 0;
					break;
				}
				if (ret == 1 || dst_meta.type() != kRedisList) {
					// Create new list for dest
					RedisListMetadata new_meta;
					dst_meta = new_meta;
					dst_meta.size = 0;
				}
			}

			uint64_t push_idx;
			if (dst_left) {
				--dst_meta.head;
				push_idx = dst_meta.head;
			} else {
				push_idx = dst_meta.tail;
				++dst_meta.tail;
			}
			++dst_meta.size;

			std::string push_index_key = encode_list_index(push_idx);
			std::string push_subkey =
			    RedisSubkeyKey::encode(_region_id, index_id, dest_slot, dest_key, dst_meta.version, push_index_key);
			batch.Put(rocks->get_data_handle(), push_subkey, popped_value);

			// Update source metadata
			if (same_key) {
				// src and dst are the same key, use dst_meta which has both changes
				if (dst_meta.size == 0) {
					batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				} else {
					std::string meta_value;
					dst_meta.encode(&meta_value);
					batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
				}
			} else {
				// Update source
				if (src_meta.size == 0) {
					batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
				} else {
					std::string src_meta_value;
					src_meta.encode(&src_meta_value);
					batch.Put(rocks->get_redis_metadata_handle(), meta_key, src_meta_value);
				}
				// Update dest
				std::string dst_meta_value;
				dst_meta.encode(&dst_meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), dest_meta_key, dst_meta_value);
			}

			// Return the popped/pushed element
			if (done != nullptr) {
				((DMLClosure*)done)->response->set_errmsg(popped_value);
			}
			num_affected = 1;
			break;
		}
		case pb::REDIS_LMPOP: {
			// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
			// We handle single-key only (multi-key routed to same region)
			// Direction: kvs[0].expire_ms bit 0 (0=LEFT, 1=RIGHT)
			// Count: from fields[0].field as string
			RedisListMetadata list_meta;
			int ret = read_list_metadata(rocks, _region_id, index_id, slot, user_key, &list_meta);
			if (ret != 0 || list_meta.type() != kRedisList || list_meta.size == 0) {
				num_affected = 0;
				break;
			}

			int64_t dir_flags = kv.has_expire_ms() ? kv.expire_ms() : 0;
			bool pop_left = (dir_flags & 1) == 0;

			int64_t count = 1;
			if (redis_req.fields_size() > 0) {
				try {
					count = std::stoll(redis_req.fields(0).field());
				} catch (...) {
					count = 1;
				}
			}
			if (count <= 0)
				count = 1;

			uint64_t pop_count = std::min(static_cast<uint64_t>(count), list_meta.size);

			std::string popped_result;
			for (uint64_t i = 0; i < pop_count; ++i) {
				uint64_t idx;
				if (pop_left) {
					idx = list_meta.head;
					++list_meta.head;
				} else {
					--list_meta.tail;
					idx = list_meta.tail;
				}
				std::string index_key = encode_list_index(idx);
				std::string subkey =
				    RedisSubkeyKey::encode(_region_id, index_id, slot, user_key, list_meta.version, index_key);

				rocksdb::ReadOptions ro;
				std::string element_value;
				auto s = rocks->get(ro, rocks->get_data_handle(), subkey, &element_value);
				if (s.ok()) {
					if (i > 0)
						popped_result += '\n';
					popped_result += element_value;
				}
				batch.Delete(rocks->get_data_handle(), subkey);
			}

			list_meta.size -= pop_count;
			if (list_meta.size == 0) {
				batch.Delete(rocks->get_redis_metadata_handle(), meta_key);
			} else {
				std::string meta_value;
				list_meta.encode(&meta_value);
				batch.Put(rocks->get_redis_metadata_handle(), meta_key, meta_value);
			}

			if (done != nullptr) {
				((DMLClosure*)done)->response->set_errmsg(popped_result);
			}
			num_affected = pop_count;
			break;
		}
		default:
			DB_WARNING("unknown redis cmd: %d, region_id: %ld", cmd, _region_id);
			break;
		}
	}

	// Commit batch
	rocksdb::WriteOptions write_options;
	auto status = rocks->write(write_options, &batch);

	if (!status.ok()) {
		DB_FATAL("Redis write failed: %s, region_id: %ld", status.ToString().c_str(), _region_id);
		if (done != nullptr) {
			((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
			((DMLClosure*)done)->response->set_errmsg(status.ToString());
			((DMLClosure*)done)->applied_index = _applied_index;
		}
		return;
	}

	_meta_writer->update_apply_index(_region_id, _applied_index, _data_index);

	if (done != nullptr) {
		((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
		((DMLClosure*)done)->response->set_affected_rows(num_affected);
		((DMLClosure*)done)->applied_index = _applied_index;
	}

	DB_DEBUG("Redis write done: cmd=%d, kvs=%d, affected=%ld, region_id=%ld, cost=%ld", cmd, redis_req.kvs_size(),
	         num_affected, _region_id, cost.get_time());
}

} // namespace neokv
