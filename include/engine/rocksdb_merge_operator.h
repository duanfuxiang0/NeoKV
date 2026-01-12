// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Stub rocksdb_merge_operator for neo-redis

#pragma once

#include "rocksdb/merge_operator.h"

namespace neokv {

// Neo-redis doesn't use SQL merge operators
// This is a stub to allow compilation

class MyMergeOperator : public rocksdb::MergeOperator {
public:
    const char* Name() const override {
        return "MyMergeOperator";
    }
    
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override {
        // Just return the last value
        if (!merge_in.operand_list.empty()) {
            merge_out->new_value = merge_in.operand_list.back().ToString();
        } else if (merge_in.existing_value) {
            merge_out->new_value = merge_in.existing_value->ToString();
        }
        return true;
    }
};

// OLAPMergeOperator - for compatibility, just an alias
using OLAPMergeOperator = MyMergeOperator;

} // namespace neokv
