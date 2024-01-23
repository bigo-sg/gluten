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

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <benchmark/benchmark.h>

using namespace local_engine;
using namespace DB;

static constexpr size_t parts_num = 3000;
static FunctionBasePtr cast_function;
static FunctionBasePtr pmod_function;

static ColumnPtr createHashColumn(size_t rows)
{
    std::srand(std::time(nullptr));
    auto hash_column = ColumnUInt64::create(rows);
    for (size_t i = 0; i < rows; i++)
        hash_column->insert(std::rand());
    return hash_column;
}

static size_t processHashColumnWithoutFunction(ColumnPtr hash_column)
{
    /// sparkMurmurHash3_32 returns are all not null.
    size_t rows = hash_column->size();
    IColumn::Selector partition_ids;
    partition_ids.reserve(rows);
    auto parts_num_int32 = static_cast<Int32>(parts_num);
    for (size_t i = 0; i < rows; i++)
    {
        // cast to int32 to be the same as the data type of the vanilla spark
        auto hash_int32 = static_cast<Int32>(hash_column->get64(i));
        auto res = hash_int32 % parts_num_int32;
        if (res < 0)
            res += parts_num_int32;
        partition_ids.emplace_back(static_cast<UInt64>(res));
    }

    size_t res = 0;
    for (auto id : partition_ids)
        res += id;
    return res;
}

static void BM_processHashColumnWithoutFunction(benchmark::State & state)
{
    auto hash_column = createHashColumn(65536);
    for (auto _ : state)
    {
        auto ret = processHashColumnWithoutFunction(hash_column);
        benchmark::DoNotOptimize(ret);
    }
}

static size_t processHashColumnWithFunction(ColumnPtr hash_column)
{
    /// sparkMurmurHash3_32 returns are all not null.
    size_t rows = hash_column->size();

    auto & factory = FunctionFactory::instance();
    auto & global_context = SerializedPlanParser::global_context;

    auto hash_result_type = std::make_shared<DataTypeUInt64>();
    ColumnsWithTypeAndName cast_args = {
        {hash_column, hash_result_type, ""}, {DataTypeString().createColumnConst(rows, "Int32"), std::make_shared<DataTypeString>(), ""}};
    if (!cast_function)
        cast_function = factory.get("CAST", global_context)->build(cast_args);
    auto cast_column = cast_function->execute(cast_args, cast_function->getResultType(), rows);

    ColumnsWithTypeAndName pmod_args
        = {{cast_column, cast_function->getResultType(), ""},
           {DataTypeUInt64().createColumnConst(rows, static_cast<UInt64>(parts_num)), std::make_shared<DataTypeUInt64>(), ""}};
    if (!pmod_function)
        pmod_function = factory.get("positiveModulo", global_context)->build(pmod_args);
    auto pmod_column = pmod_function->execute(pmod_args, pmod_function->getResultType(), rows);

    const auto * pmod_col = checkAndGetColumn<ColumnUInt64>(pmod_column.get());
    if (!pmod_col)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Wrong type of selector column:{} expect ColumnUInt64", pmod_col->getName());

    size_t res = 0;
    for (auto id : pmod_col->getData())
        res += id;
    return res;
}

static void BM_processHashColumnWithFunction(benchmark::State & state)
{
    auto hash_column = createHashColumn(65536);
    for (auto _ : state)
    {
        auto ret = processHashColumnWithFunction(hash_column);
        benchmark::DoNotOptimize(ret);
    }
}

BENCHMARK(BM_processHashColumnWithoutFunction);
BENCHMARK(BM_processHashColumnWithFunction);

