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
#include "redis_service.h"

namespace neokv {

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

	for (const auto& kv : redis_req.kvs()) {
		const std::string& user_key = kv.key();
		uint16_t slot = redis_slot(user_key);

		std::string rocks_key = RedisCodec::encode_key(_region_id, index_id, slot, user_key, REDIS_STRING);

		switch (cmd) {
		case pb::REDIS_SET:
		case pb::REDIS_MSET: {
			if (cmd == pb::REDIS_SET && set_condition != pb::REDIS_SET_CONDITION_NONE) {
				rocksdb::ReadOptions read_options;
				std::string existing_value;
				bool key_exists = false;
				auto read_status = rocks->get(read_options, rocks->get_data_handle(), rocks_key, &existing_value);
				if (read_status.ok()) {
					int64_t expire_at = 0;
					std::string payload;
					if (!RedisCodec::decode_value(existing_value, &expire_at, &payload)) {
						DB_WARNING("decode redis value failed, region_id: %ld", _region_id);
						if (done != nullptr) {
							((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
							((DMLClosure*)done)->response->set_errmsg("decode redis value failed");
							((DMLClosure*)done)->applied_index = _applied_index;
						}
						return;
					}
					key_exists = !RedisCodec::is_expired(expire_at, RedisCodec::current_time_ms());
				} else if (!read_status.IsNotFound()) {
					DB_WARNING("redis pre-read failed: %s, region_id: %ld", read_status.ToString().c_str(), _region_id);
					if (done != nullptr) {
						((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
						((DMLClosure*)done)->response->set_errmsg(read_status.ToString());
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
			std::string rocks_value = RedisCodec::encode_value(expire_ms, kv.value());
			batch.Put(rocks->get_data_handle(), rocks_key, rocks_value);
			++num_affected;
			break;
		}
		case pb::REDIS_DEL: {
			// Check if key exists before delete (for counting)
			rocksdb::ReadOptions read_options;
			std::string existing_value;
			auto status = rocks->get(read_options, rocks->get_data_handle(), rocks_key, &existing_value);
			if (status.ok()) {
				// Check if expired
				int64_t expire_at = 0;
				std::string payload;
				if (RedisCodec::decode_value(existing_value, &expire_at, &payload) &&
				    !RedisCodec::is_expired(expire_at, RedisCodec::current_time_ms())) {
					batch.Delete(rocks->get_data_handle(), rocks_key);
					++num_affected;
				}
			}
			break;
		}
		case pb::REDIS_EXPIRE:
		case pb::REDIS_EXPIREAT: {
			// Read existing value, update expire time
			rocksdb::ReadOptions read_options;
			std::string existing_value;
			auto status = rocks->get(read_options, rocks->get_data_handle(), rocks_key, &existing_value);
			if (status.ok()) {
				int64_t old_expire = 0;
				std::string payload;
				if (RedisCodec::decode_value(existing_value, &old_expire, &payload) &&
				    !RedisCodec::is_expired(old_expire, RedisCodec::current_time_ms())) {
					// Update with new expire time
					int64_t new_expire = kv.expire_ms();
					std::string new_value = RedisCodec::encode_value(new_expire, payload);
					batch.Put(rocks->get_data_handle(), rocks_key, new_value);
					++num_affected;
				}
			}
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
