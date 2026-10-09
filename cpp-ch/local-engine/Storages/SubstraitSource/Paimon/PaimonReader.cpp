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

#include "PaimonReader.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Common/BlockTypeUtils.h>
#include <Common/Exception.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <Storages/SubstraitSource/Paimon/Bridge/GlutenPaimonBridge.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace local_engine::paimon
{
namespace
{
class PaimonBridgeBlockInputFormat final : public DB::IInputFormat
{
    GlutenPaimonReaderHandle * handle_ = nullptr;
    DB::ArrowColumnToCHColumn arrow_to_ch_;
    bool eof_ = false;

public:
    PaimonBridgeBlockInputFormat(
        DB::ReadBuffer * in_,
        const DB::SharedHeader & header_,
        GlutenPaimonReaderHandle * handle,
        DB::ContextPtr context)
        : DB::IInputFormat(header_, in_)
        , handle_(handle)
        , arrow_to_ch_(
              [&]
              {
                  const DB::FormatSettings format_settings = getFormatSettings(context);
                  return DB::ArrowColumnToCHColumn(
                      *header_,
                      "Paimon",
                      format_settings,
                      std::nullopt,
                      std::nullopt,
                      format_settings.parquet.allow_missing_columns,
                      format_settings.null_as_default,
                      format_settings.date_time_overflow_behavior,
                      format_settings.parquet.allow_geoparquet_parser,
                      format_settings.parquet.case_insensitive_column_matching,
                      false,
                      format_settings.parquet.enable_json_parsing);
              }())
    {
    }

    ~PaimonBridgeBlockInputFormat() override
    {
        if (handle_)
            glutenPaimonReaderClose(handle_);
        handle_ = nullptr;
    }

    String getName() const override { return "PaimonBridgeBlockInputFormat"; }

protected:
    DB::Chunk read() override
    {
        if (eof_ || !handle_)
            return {};

        ArrowArray c_array{};
        ArrowSchema c_schema{};
        char err[512]{};
        const auto status = glutenPaimonReaderNextBatch(handle_, &c_array, &c_schema, err, sizeof(err));
        if (status == GLUTEN_PAIMON_EOF)
        {
            eof_ = true;
            glutenPaimonReaderClose(handle_);
            handle_ = nullptr;
            return {};
        }
        if (status != GLUTEN_PAIMON_OK)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Paimon bridge reader: {}", err);

        SCOPE_EXIT({
            if (c_array.release)
                c_array.release(&c_array);
            if (c_schema.release)
                c_schema.release(&c_schema);
        });

        auto import_result = arrow::ImportArray(&c_array, &c_schema);
        if (!import_result.ok())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Paimon bridge Arrow import: {}", import_result.status().ToString());

        const auto struct_array = std::static_pointer_cast<arrow::StructArray>(import_result.ValueOrDie());
        const auto struct_type = std::static_pointer_cast<arrow::StructType>(struct_array->type());
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(struct_type->num_fields());
        for (int i = 0; i < struct_type->num_fields(); ++i)
            columns.push_back(struct_array->field(i));
        auto batch = arrow::RecordBatch::Make(arrow::schema(struct_type->fields()), struct_array->length(), columns);
        auto table = arrow::Table::FromRecordBatches({batch}).ValueOrDie();
        DB::Chunk chunk = arrow_to_ch_.arrowTableToCHChunk(table, table->num_rows(), nullptr, nullptr);
        if (chunk.getNumRows() == 0)
            return read();
        return chunk;
    }
};

FormatFile::InputFormatPtr createPaimonInputFormat(const FormatFilePtr & file, const DB::Block & header)
{
    const auto & file_info = file->getFileInfo();
    if (!file_info.has_paimon())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "PaimonReader requires paimon read options in LocalFiles");

    const auto & paimon_options = file_info.paimon();
    const auto & serialized_split = paimon_options.serialized_split();
    const auto & table_path = paimon_options.table_path();

    char err[512]{};
    auto * handle = glutenPaimonReaderOpen(
        table_path.c_str(),
        reinterpret_cast<const uint8_t *>(serialized_split.data()),
        serialized_split.size(),
        err,
        sizeof(err));
    if (!handle)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Paimon bridge open: {}", err);

    static const std::string empty_read_buffer;
    auto read_buffer = std::make_unique<DB::ReadBufferFromString>(empty_read_buffer);
    auto input = std::make_shared<PaimonBridgeBlockInputFormat>(
        read_buffer.get(), toShared(header), handle, file->getContext());
    return std::make_shared<FormatFile::InputFormat>(std::move(read_buffer), input);
}
}

std::unique_ptr<PaimonReader> PaimonReader::create(
    const FormatFilePtr & file_, const DB::Block & to_read_header_, const DB::Block & output_header_)
{
    auto input = createPaimonInputFormat(file_, to_read_header_);
    if (!input)
        return nullptr;
    return std::make_unique<PaimonReader>(file_, to_read_header_, output_header_, input);
}

PaimonReader::PaimonReader(
    const FormatFilePtr & file_,
    const DB::Block & to_read_header_,
    const DB::Block & output_header_,
    const FormatFile::InputFormatPtr & input_format_)
    : NormalFileReader(file_, to_read_header_, output_header_, input_format_)
{
}

}
