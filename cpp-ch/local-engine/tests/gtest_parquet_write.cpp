#include "Processors/Formats/Impl/ArrowColumnToCHColumn.h"
#include "config.h"

#if USE_PARQUET && USE_ARROW
#    include <iostream>
#    include <Core/Block.h>
#    include <Core/ColumnWithTypeAndName.h>
#    include <Core/Field.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#    include <arrow/table.h>
#    include <gtest/gtest.h>

using namespace DB;

template <typename Column>
static void printColumns(const std::vector<Column> & columns)
{
    for (size_t i = 0; i < columns.size(); ++i)
        for (size_t j = 0; j < columns[i]->size(); ++j)
            std::cout << "col:" << i << ",row:" << j << "," << toString((*columns[i])[j]) << std::endl;
}

TEST(ParquetWrite, ComplexTypes)
{
    Block header;

    /// map field
    {
        ColumnWithTypeAndName col;
        col.name = "map_field";
        String str_type = "Nullable(Map(Int32, Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// map field with null
    {
        ColumnWithTypeAndName col;
        col.name = "map_field_with_null";
        String str_type = "Nullable(Map(Int32, Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// map field with empty map
    {
        ColumnWithTypeAndName col;
        col.name = "map_field_with_empty_map";
        String str_type = "Nullable(Map(Int32, Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// array field
    {
        ColumnWithTypeAndName col;
        col.name = "array_field";
        String str_type = "Nullable(Array(Nullable(Int32)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// array field with null
    {
        ColumnWithTypeAndName col;
        col.name = "array_field_with_null";
        String str_type = "Nullable(Array(Nullable(Int32)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// array field with empty array
    {
        ColumnWithTypeAndName col;
        col.name = "array_field_with_empty_array";
        String str_type = "Nullable(Array(Nullable(Int32)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// struct_field
    {
        ColumnWithTypeAndName col;
        col.name = "struct_field";
        String str_type = "Nullable(Tuple(Nullable(Int32), Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// struct field with null
    {
        ColumnWithTypeAndName col;
        col.name = "struct_field_with_null";
        String str_type = "Nullable(Tuple(Nullable(Int32), Nullable(Int64)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    Block block = header.cloneEmpty();
    auto columns = block.mutateColumns();

    size_t rows = 10;
    for (size_t i = 0; i < rows; i++)
    {
        /// map_field
        {
            Map map;

            Tuple tuple1{Int32(i), Int64(i + 1)};
            map.emplace_back(std::move(tuple1));

            Tuple tuple2{Int32(i + 2), Int64(i + 3)};
            map.emplace_back(std::move(tuple2));

            columns[0]->insert(std::move(map));
        }

        /// map_field_with_null
        {
            columns[1]->insert({});
        }

        /// map_field_with_empty_map
        {
            columns[2]->insert(Map{});
        }

        /// array_field
        {
            Array array;
            array.emplace_back(Int32(i));
            array.emplace_back(Int32(i + 1));
            columns[3]->insert(std::move(array));
        }

        /// array_field_with_null
        {
            columns[4]->insert({});
        }

        /// array_field_with_empty_array
        {
            columns[5]->insert(Array{});
        }

        /// struct_field
        {
            Tuple tuple;
            tuple.emplace_back(Int32(i));
            tuple.emplace_back(Int64(i + 1));
            columns[6]->insert(std::move(tuple));
        }

        /// struct_field_with_null
        {
            columns[7]->insert({});
        }
    }
    std::cout << "input block:" << std::endl;
    printColumns(columns);

    /// Convert CH Block to Arrow Table
    std::shared_ptr<arrow::Table> arrow_table;
    CHColumnToArrowColumn ch2arrow(header, "Parquet", false, true, true);
    Chunk chunk{std::move(columns), rows};
    std::vector<Chunk> chunks;
    chunks.push_back(std::move(chunk));
    ch2arrow.chChunkToArrowTable(arrow_table, chunks, block.columns());
    std::cout << "arrow_table->num_rows() = " << arrow_table->num_rows() << std::endl;
    std::cout << "arrow_table->ToString() = " << arrow_table->ToString() << std::endl;

    /// Convert Arrow Table to CH Block
    ArrowColumnToCHColumn arrow2ch(header, "Parquet", true, true, true);
    Chunk res;
    arrow2ch.arrowTableToCHChunk(res, arrow_table, arrow_table->num_rows());

    std::cout << "output block:" << std::endl;
    printColumns(res.getColumns());
}

#endif
