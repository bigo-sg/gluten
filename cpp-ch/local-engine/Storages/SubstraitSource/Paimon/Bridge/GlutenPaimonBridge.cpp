/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define GLUTEN_PAIMON_BRIDGE_EXPORT __attribute__((visibility("default")))

#include "GlutenPaimonBridge.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/read_context.h>
#include <paimon/reader/batch_reader.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_read.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace
{
void setError(char * err_buf, size_t err_buf_len, const std::string & msg)
{
    if (!err_buf || err_buf_len == 0)
        return;
    std::strncpy(err_buf, msg.c_str(), err_buf_len - 1);
    err_buf[err_buf_len - 1] = '\0';
}

template <typename T>
T takeResult(::paimon::Result<T> && result)
{
    if (!result.ok())
        throw std::runtime_error(result.status().ToString());
    return std::move(result).value();
}

std::shared_ptr<arrow::Array> removeValueKindColumn(const std::shared_ptr<arrow::Array> & array)
{
    if (array->type_id() != arrow::Type::STRUCT)
        throw std::runtime_error("Paimon bridge expects struct Arrow batch");

    const auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
    const auto struct_type = std::static_pointer_cast<arrow::StructType>(array->type());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (int i = 0; i < struct_type->num_fields(); ++i)
    {
        if (struct_type->field(i)->name() == "_VALUE_KIND")
            continue;
        fields.push_back(struct_type->field(i));
        columns.push_back(struct_array->field(i));
    }
    if (fields.empty())
        throw std::runtime_error("Paimon bridge batch has no data columns");

    auto batch = arrow::RecordBatch::Make(arrow::schema(fields), struct_array->length(), columns);
    return batch->ToStructArray().ValueOrDie();
}

struct PaimonBridgeReaderImpl
{
    std::unique_ptr<::paimon::BatchReader> batch_reader;
};
}

extern "C" {

GLUTEN_PAIMON_BRIDGE_EXPORT GlutenPaimonReaderHandle * glutenPaimonReaderOpen(
    const char * table_path, const uint8_t * split_bytes, size_t split_len, char * err_buf, size_t err_buf_len)
{
    try
    {
        if (!table_path || !split_bytes)
        {
            setError(err_buf, err_buf_len, "table_path and split_bytes are required");
            return nullptr;
        }

        auto memory_pool = ::paimon::GetDefaultPool();
        auto split = takeResult(::paimon::Split::Deserialize(
            reinterpret_cast<const char *>(split_bytes), split_len, memory_pool));

        ::paimon::ReadContextBuilder builder(table_path);
        auto read_context = takeResult(builder.Finish());
        auto table_read = takeResult(::paimon::TableRead::Create(std::move(read_context)));
        auto batch_reader = takeResult(table_read->CreateReader(split));

        auto * handle = new PaimonBridgeReaderImpl();
        handle->batch_reader = std::move(batch_reader);
        return reinterpret_cast<GlutenPaimonReaderHandle *>(handle);
    }
    catch (const std::exception & e)
    {
        setError(err_buf, err_buf_len, e.what());
        return nullptr;
    }
}

GLUTEN_PAIMON_BRIDGE_EXPORT enum GlutenPaimonStatus glutenPaimonReaderNextBatch(
    GlutenPaimonReaderHandle * reader,
    struct ArrowArray * out_array,
    struct ArrowSchema * out_schema,
    char * err_buf,
    size_t err_buf_len)
{
    auto * impl = reinterpret_cast<PaimonBridgeReaderImpl *>(reader);
    if (!impl || !impl->batch_reader)
    {
        setError(err_buf, err_buf_len, "invalid reader handle");
        return GLUTEN_PAIMON_ERROR;
    }
    try
    {
        auto batch = takeResult(impl->batch_reader->NextBatch());
        if (::paimon::BatchReader::IsEofBatch(batch))
        {
            impl->batch_reader->Close();
            return GLUTEN_PAIMON_EOF;
        }

        auto import_status = arrow::ImportArray(batch.first.get(), batch.second.get());
        if (!import_status.ok())
        {
            setError(err_buf, err_buf_len, import_status.status().ToString());
            return GLUTEN_PAIMON_ERROR;
        }

        auto array = removeValueKindColumn(import_status.ValueUnsafe());
        const auto export_status = arrow::ExportArray(*array, out_array, out_schema);
        if (!export_status.ok())
        {
            setError(err_buf, err_buf_len, export_status.ToString());
            return GLUTEN_PAIMON_ERROR;
        }
        return GLUTEN_PAIMON_OK;
    }
    catch (const std::exception & e)
    {
        setError(err_buf, err_buf_len, e.what());
        return GLUTEN_PAIMON_ERROR;
    }
}

GLUTEN_PAIMON_BRIDGE_EXPORT void glutenPaimonReaderClose(GlutenPaimonReaderHandle * reader)
{
    delete reinterpret_cast<PaimonBridgeReaderImpl *>(reader);
}

}
