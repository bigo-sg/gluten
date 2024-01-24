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

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>
#include <Functions/SparkFunctionFloor.h>
#include <Parser/SerializedPlanParser.h>
#include <benchmark/benchmark.h>
#include <Common/TargetSpecific.h>

using namespace DB;

static Block createDataBlock(String type_str, size_t rows)
{
    auto type = DataTypeFactory::instance().get(type_str);
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        if (i % 100)
        {
            column->insertDefault();
        }
        else if (isInt(type))
        {
            column->insert(i);
        }
        else if (isFloat(type))
        {
            double d = i * 1.0;
            column->insert(d);
        }
    }
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, "d"));
    return std::move(block);
}

static void BM_CHFloorFunction_For_Int64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("floor", local_engine::SerializedPlanParser::global_context);
    Block int64_block = createDataBlock("Nullable(Int64)", 65536);
    auto executable = function->build(int64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(int64_block.getColumnsWithTypeAndName(), executable->getResultType(), int64_block.rows());
        benchmark::DoNotOptimize(result);
    }
}

static void BM_CHFloorFunction_For_Float64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("floor", local_engine::SerializedPlanParser::global_context);
    Block float64_block = createDataBlock("Nullable(Float64)", 65536);
    auto executable = function->build(float64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(float64_block.getColumnsWithTypeAndName(), executable->getResultType(), float64_block.rows());
        benchmark::DoNotOptimize(result);
    }
}

static void BM_SparkFloorFunction_For_Int64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkFloor", local_engine::SerializedPlanParser::global_context);
    Block int64_block = createDataBlock("Nullable(Int64)", 65536);
    auto executable = function->build(int64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(int64_block.getColumnsWithTypeAndName(), executable->getResultType(), int64_block.rows());
        benchmark::DoNotOptimize(result);
    }
}

static void BM_SparkFloorFunction_For_Float64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkFloor", local_engine::SerializedPlanParser::global_context);
    Block float64_block = createDataBlock("Nullable(Float64)", 65536);
    auto executable = function->build(float64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(float64_block.getColumnsWithTypeAndName(), executable->getResultType(), float64_block.rows());
        benchmark::DoNotOptimize(result);
    }
}

static void nanInfToNullAutoOpt(float * data, uint8_t * null_map, size_t size)
{
    for (size_t i = 0; i < size; ++i)
    {
        uint8_t is_nan = (data[i] != data[i]);
        uint8_t is_inf = ((*reinterpret_cast<const uint32_t *>(&data[i]) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000);
        uint8_t null_flag = is_nan | is_inf;
        null_map[i] = null_flag;

        UInt32 * uint_data = reinterpret_cast<UInt32 *>(&data[i]);
        *uint_data &= ~(-null_flag);
    }
}

static void BMNanInfToNullAutoOpt(benchmark::State & state)
{
    constexpr size_t size = 8192;
    float data[size];
    uint8_t null_map[size] = {0};
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<float>(rand()) / rand();

    for (auto _ : state)
    {
        nanInfToNullAutoOpt(data, null_map, size);
        benchmark::DoNotOptimize(null_map);
    }
}
BENCHMARK(BMNanInfToNullAutoOpt);

DECLARE_AVX2_SPECIFIC_CODE(

    void nanInfToNullSIMD(float * data, uint8_t * null_map, size_t size) {
        const __m256 inf = _mm256_set1_ps(INFINITY);
        const __m256 neg_inf = _mm256_set1_ps(-INFINITY);
        const __m256 zero = _mm256_set1_ps(0.0f);

        size_t i = 0;
        for (; i + 7 < size; i += 8)
        {
            __m256 values = _mm256_loadu_ps(&data[i]);

            __m256 is_inf = _mm256_cmp_ps(values, inf, _CMP_EQ_OQ);
            __m256 is_neg_inf = _mm256_cmp_ps(values, neg_inf, _CMP_EQ_OQ);
            __m256 is_nan = _mm256_cmp_ps(values, values, _CMP_NEQ_UQ);
            __m256 is_null = _mm256_or_ps(_mm256_or_ps(is_inf, is_neg_inf), is_nan);
            __m256 new_values = _mm256_blendv_ps(values, zero, is_null);

            _mm256_storeu_ps(&data[i], new_values);

            UInt32 mask = static_cast<UInt32>(_mm256_movemask_ps(is_null));
            for (size_t j = 0; j < 8; ++j)
            {
                UInt8 null_flag = (mask & 1U);
                null_map[i + j] = null_flag;
                mask >>= 1;
            }
        }
    }
)

static void BMNanInfToNullAVX2(benchmark::State & state)
{
    constexpr size_t size = 8192;
    float data[size];
    uint8_t null_map[size] = {0};
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<float>(rand()) / rand();

    for (auto _ : state)
    {
        ::TargetSpecific::AVX2::nanInfToNullSIMD(data, null_map, size);
        benchmark::DoNotOptimize(null_map);
    }
}
BENCHMARK(BMNanInfToNullAVX2);

static void nanInfToNull(float * data, uint8_t * null_map, size_t size)
{
    for (size_t i = 0; i < size; ++i)
    {
        if (data[i] != data[i])
            null_map[i] = 1;
        else if ((*reinterpret_cast<const uint32_t *>(&data[i]) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000)
            null_map[i] = 1;
        else
            null_map[i] = 0;

        if (null_map[i])
            data[i] = 0.0;
    }
}

static void BMNanInfToNull(benchmark::State & state)
{
    constexpr size_t size = 8192;
    float data[size];
    uint8_t null_map[size] = {0};
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<float>(rand()) / rand();

    for (auto _ : state)
    {
        nanInfToNull(data, null_map, size);
        benchmark::DoNotOptimize(null_map);
    }
}
BENCHMARK(BMNanInfToNull);

BENCHMARK(BM_CHFloorFunction_For_Int64);
BENCHMARK(BM_CHFloorFunction_For_Float64);
BENCHMARK(BM_SparkFloorFunction_For_Int64);
BENCHMARK(BM_SparkFloorFunction_For_Float64);


/*
/// TO run in https://quick-bench.com/q/h-2qGgqxM8ksp57VD0w7JdKKN-I
using UInt8 = unsigned char;
using UInt64 = unsigned long long;
using Int64 = signed long long;
template<typename T>
using PaddedPODArray = std::vector<T>;
*/

/*
Benchmark when BranchType is Float64
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1     414177 ns       414161 ns         1687
BM_fillVectorVector2      96669 ns        96665 ns         7432
BM_fillVectorVector3      78439 ns        78436 ns         8812

Benchmark when BranchType is Int64
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1      80645 ns        80643 ns         8101
BM_fillVectorVector2      73841 ns        73838 ns         9484
BM_fillVectorVector3      73883 ns        73881 ns         9485

Benchmark when BranchType is Decimal64
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1      82413 ns        82408 ns         8635
BM_fillVectorVector2      76289 ns        76287 ns         9213
BM_fillVectorVector3      76262 ns        76260 ns         9244

Benchmark when BranchType is Int256
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1     307741 ns       307726 ns         2263
BM_fillVectorVector2    2184999 ns      2184903 ns          321
BM_fillVectorVector3     318616 ns       318605 ns         2209

Benchmark when BranchType is Decimal256
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1     303179 ns       303164 ns         2311
BM_fillVectorVector3     305023 ns       305010 ns         2266
*/

/*
Some commands that would be helpful

# run benchmark
./build_gcc/utils/extern-local-engine/tests/benchmark_local_engine --benchmark_filter="BM_fillVectorVector*"

# get full symbol name
objdump -t  ./build_gcc/utils/extern-local-engine/tests/benchmark_local_engine   |  c++filt   | grep "fillVectorVector1"

# get assembly code mixed with source code by symbol name
gdb -batch -ex "disassemble/rs 'void fillVectorVector3<double, double>(DB::PODArray<char8_t, 4096ul, Allocator<false, false>, 63ul, 64ul> const&, DB::PODArray<double, 4096ul, Allocator<false, false>, 63ul, 64ul> const&, DB::PODArray<double, 4096ul, Allocator<false, false>, 63ul, 64ul> const&, DB::PODArray<double, 4096ul, Allocator<false, false>, 63ul, 64ul>&)'" ./build_gcc/utils/extern-local-engine/tests/benchmark_local_engine    | c++filt  > 3.S
*/

using ResultType = Decimal64;

template <typename Branch1Type = ResultType, typename Branch2Type = ResultType>
static NO_INLINE void fillVectorVector0(const PaddedPODArray<UInt8> & cond, Branch1Type a, Branch2Type b, PaddedPODArray<ResultType> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
    }
}

template <typename Branch1Type = ResultType, typename Branch2Type = ResultType>
static NO_INLINE void fillVectorVector1(
    const PaddedPODArray<UInt8> & cond,
    const PaddedPODArray<Branch1Type> & a,
    const PaddedPODArray<Branch2Type> & b,
    PaddedPODArray<ResultType> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
    }
}

template <typename Branch1Type = ResultType, typename Branch2Type = ResultType>
static NO_INLINE void fillVectorVector2(
    const PaddedPODArray<UInt8> & cond,
    const PaddedPODArray<Branch1Type> & a,
    const PaddedPODArray<Branch2Type> & b,
    PaddedPODArray<ResultType> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        res[i] = (!!cond[i]) * static_cast<ResultType>(a[i]) + (!cond[i]) * static_cast<ResultType>(b[i]);
    }
}

template <typename Branch1Type = ResultType, typename Branch2Type = ResultType>
static NO_INLINE void fillVectorVector3(
    const PaddedPODArray<UInt8> & cond,
    const PaddedPODArray<Branch1Type> & a,
    const PaddedPODArray<Branch2Type> & b,
    PaddedPODArray<ResultType> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (std::is_integral_v<ResultType> || (is_decimal<ResultType> && sizeof(ResultType) <= 8))
        {
            res[i] = (!!cond[i]) * static_cast<ResultType>(a[i]) + (!cond[i]) * static_cast<ResultType>(b[i]);
        }
        else if constexpr (std::is_floating_point_v<ResultType>)
        {
            using UIntType = std::conditional_t<sizeof(ResultType) == 8, UInt64, UInt32>;
            using IntType = std::conditional_t<sizeof(ResultType) == 8, Int64, Int32>;
            auto mask = static_cast<UIntType>(static_cast<IntType>(cond[i]) - 1);
            auto new_a = static_cast<ResultType>(a[i]);
            auto new_b = static_cast<ResultType>(b[i]);
            auto tmp = (~mask & (*reinterpret_cast<const UIntType *>(&new_a))) | (mask & (*reinterpret_cast<const UIntType *>(&new_b)));
            res[i] = *(reinterpret_cast<ResultType *>(&tmp));
        }
        else
        {
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        }
    }
}

static constexpr size_t ROWS = 65536;
static void initCondition(PaddedPODArray<UInt8> & cond)
{
    cond.resize(ROWS);
    for (size_t i = 0; i < ROWS; ++i)
    {
        cond[i] = std::rand() % 2;
    }
}

template <typename T>
static void initBranch(PaddedPODArray<T> & branch)
{
    branch.resize(ROWS);
    for (size_t i = 0; i < ROWS; ++i)
    {
        branch[i] = static_cast<T>(std::rand());
    }
}


static void BM_fillVectorVector0(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    ResultType a = std::rand();
    ResultType b = std::rand();
    PaddedPODArray<ResultType> res(ROWS);
    initCondition(cond);

    for (auto _ : state)
    {
        fillVectorVector0(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_fillVectorVector1(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    PaddedPODArray<ResultType> a;
    PaddedPODArray<ResultType> b;
    PaddedPODArray<ResultType> res(ROWS);
    initCondition(cond);
    initBranch(a);
    initBranch(b);

    for (auto _ : state)
    {
        fillVectorVector1(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_fillVectorVector2(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    PaddedPODArray<ResultType> a;
    PaddedPODArray<ResultType> b;
    PaddedPODArray<ResultType> res(ROWS);
    initCondition(cond);
    initBranch(a);
    initBranch(b);

    for (auto _ : state)
    {
        fillVectorVector2(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_fillVectorVector3(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    PaddedPODArray<ResultType> a;
    PaddedPODArray<ResultType> b;
    PaddedPODArray<ResultType> res(ROWS);
    initCondition(cond);
    initBranch(a);
    initBranch(b);

    for (auto _ : state)
    {
        fillVectorVector3(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

BENCHMARK(BM_fillVectorVector0);
BENCHMARK(BM_fillVectorVector1);
BENCHMARK(BM_fillVectorVector2);
BENCHMARK(BM_fillVectorVector3);