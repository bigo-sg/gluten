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
Test performance of fillConstantConstant*
Benchmark when BranchType is Int64
-------------------------------------------------------------------
Benchmark                         Time             CPU   Iterations
-------------------------------------------------------------------
BM_fillConstantConstant1      31360 ns        31359 ns        22249
BM_fillConstantConstant2      31369 ns        31368 ns        22288
BM_fillConstantConstant3      31583 ns        31581 ns        22254
*/

/*
Test performance of fillVectorVector*
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

using ResultType = Float64;

template <typename T>
static NO_INLINE void fillConstantConstant1(const PaddedPODArray<UInt8> & cond, T a, T b, PaddedPODArray<T> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        res[i] = cond[i] ? static_cast<T>(a) : static_cast<T>(b);
    }
}

template <typename T>
static NO_INLINE void
fillConstantConstant3(const PaddedPODArray<UInt8> & cond, T a, T b, PaddedPODArray<T> & res)
{
    size_t rows = cond.size();
    T new_a = static_cast<T>(a);
    T new_b = static_cast<T>(b);
    alignas(64) const T ab[2] = {new_a, new_b};
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (std::is_integral_v<T> && sizeof(T) == 1)
        {
            /// auto opt: cmove and simd is used for integral types
            // res[i] = cond[i] ? new_a : new_b;
            res[i] = ab[!cond[i]];
        }
        else if constexpr (std::is_floating_point_v<T>)
        {
            /// auto opt: cmove not used but simd is used for floating point types
            res[i] = cond[i] ? new_a : new_b;
        }
        else if constexpr (is_decimal<T> && sizeof(T) <= 8)
        {
            /// auto opt: simd is used for decimal types
            res[i] = cond[i] ? new_a : new_b;
        }
        else if constexpr (is_decimal<T> && sizeof(T) == 32)
        {
            /// avoid branch mispredict
            res[i] = ab[!cond[i]];
        }
        else if constexpr (is_decimal<T> && sizeof(T) == 16)
        {
            /// auto opt: cmove and loop unrolling
            // res[i] = cond[i] ? static_cast<T>(a) : static_cast<T>(b);
            res[i] = ab[!cond[i]];
        }
        else if constexpr (is_big_int_v<T> && sizeof(T) == 32)
        {
            res[i] = ab[!cond[i]];
        }
        else if constexpr (is_big_int_v<T> && sizeof(T) == 16)
        {
            // res[i] = cond[i] ? static_cast<T>(a) : static_cast<T>(b);
            res[i] = ab[!cond[i]];
        }
        else
        {
            res[i] = cond[i] ? static_cast<T>(a) : static_cast<T>(b);
        }
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
        // res[i] = (!!cond[i]) * static_cast<ResultType>(a[i]) + (!cond[i]) * static_cast<ResultType>(b[i]);
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
            // res[i] = (!!cond[i]) * static_cast<ResultType>(a[i]) + (!cond[i]) * static_cast<ResultType>(b[i]);
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

template <typename T = ResultType>
static void BM_fillConstantConstant1(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    T a(std::rand());
    T b(std::rand());
    PaddedPODArray<T> res(ROWS);
    initCondition(cond);

    for (auto _ : state)
    {
        fillConstantConstant1(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

template <typename T = ResultType>
static void BM_fillConstantConstant3(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    T a(std::rand());
    T b(std::rand());
    PaddedPODArray<T> res(ROWS);
    initCondition(cond);

    for (auto _ : state)
    {
        fillConstantConstant3(cond, a, b, res);
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

/*
-------------------------------------------------------------------------------
Benchmark                                     Time             CPU   Iterations
-------------------------------------------------------------------------------
BM_fillConstantConstant1<Int8>           492635 ns       492619 ns         1415
BM_fillConstantConstant3<Int8>            80339 ns        80336 ns         8803
BM_fillConstantConstant1<Int16>            7899 ns         7899 ns        88745
BM_fillConstantConstant3<Int16>            7903 ns         7903 ns        88738
BM_fillConstantConstant1<Int32>           15704 ns        15703 ns        44615
BM_fillConstantConstant3<Int32>           15849 ns        15848 ns        44592
BM_fillConstantConstant1<Int64>           31443 ns        31442 ns        22226
BM_fillConstantConstant3<Int64>           31407 ns        31406 ns        22304
BM_fillConstantConstant1<Int128>          95711 ns        95709 ns         7317
BM_fillConstantConstant3<Int128>          91466 ns        91463 ns         7657
BM_fillConstantConstant1<Int256>         565219 ns       565201 ns         1233
BM_fillConstantConstant3<Int256>         131145 ns       131140 ns         5350
BM_fillConstantConstant1<Float32>         15768 ns        15768 ns        44554
BM_fillConstantConstant3<Float32>         15685 ns        15684 ns        44597
BM_fillConstantConstant1<Float64>         31377 ns        31376 ns        22281
BM_fillConstantConstant3<Float64>         31367 ns        31366 ns        22307
BM_fillConstantConstant1<Decimal32>       65185 ns        65182 ns        10912
BM_fillConstantConstant3<Decimal32>       15703 ns        15702 ns        44490
BM_fillConstantConstant1<Decimal64>       64509 ns        64507 ns        10875
BM_fillConstantConstant3<Decimal64>       31839 ns        31838 ns        22305
BM_fillConstantConstant1<Decimal128>      95602 ns        95600 ns         7325
BM_fillConstantConstant3<Decimal128>      91615 ns        91612 ns         7646
BM_fillConstantConstant1<Decimal256>     572220 ns       572208 ns         1234
BM_fillConstantConstant3<Decimal256>     130326 ns       130323 ns         5375
BM_fillConstantConstant1<DateTime64>      64597 ns        64596 ns        10844
BM_fillConstantConstant3<DateTime64>      64964 ns        64963 ns        10885
*/
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt8);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt8);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int8);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int8);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt16);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt16);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int16);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int16);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Float32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Float32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Float64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Float64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Decimal32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Decimal32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Decimal64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Decimal64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Decimal128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Decimal128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Decimal256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Decimal256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, DateTime64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, DateTime64);

BENCHMARK(BM_fillVectorVector1);
BENCHMARK(BM_fillVectorVector2);
BENCHMARK(BM_fillVectorVector3);


template <typename T>
struct slice
{
    T * data;
};

template <typename T>
NO_INLINE auto BitOrProcess(slice<T> & d)
{
    for (auto i = 0u; i < 65536; ++i)
        d.data[i] |= T(0xaa);
}

template <typename T>
void initSlice(slice<T> & d)
{
    d.data = new T[65536];
    for (auto i = 0u; i < 65536; ++i)
        d.data[i] = T(std::rand());
}

template <typename T>
void finalizeSlice(slice<T> & d)
{
    delete[] d.data;
}

template <typename T>
void BM_BitOrProcess(benchmark::State & state)
{
    slice<T> d;
    initSlice(d);

    for (auto _ : state)
    {
        BitOrProcess(d);
        benchmark::DoNotOptimize(d);
    }
}

BENCHMARK_TEMPLATE(BM_BitOrProcess, char8_t);
BENCHMARK_TEMPLATE(BM_BitOrProcess, int8_t);
BENCHMARK_TEMPLATE(BM_BitOrProcess, uint8_t);
