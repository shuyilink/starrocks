// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "common/status.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>


namespace starrocks {

class Field;
class TypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;

class InvertedIndexColumnWriter {
public:
    static Status create(const Field* field, const std::string& segment_file_name,
                         std::unique_ptr<InvertedIndexColumnWriter>* res);
    virtual Status init() = 0;

    InvertedIndexColumnWriter() = default;
    virtual ~InvertedIndexColumnWriter() = default;

    InvertedIndexColumnWriter(const InvertedIndexColumnWriter &) =delete;
    InvertedIndexColumnWriter(InvertedIndexColumnWriter &&) =delete;

    virtual Status add_values(const void* values, size_t count) = 0;
    virtual Status add_nulls(uint32_t count) = 0;
    virtual Status finish() = 0;

    virtual int64_t size() const = 0;
    virtual int64_t file_size() const = 0;
};

} // namespace starrocks
