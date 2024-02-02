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

#include <immintrin.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <base/Decimal.h>
#include <base/extended_types.h>
#include <benchmark/benchmark.h>
#include <Common/PODArray.h>
#include <Common/TargetSpecific.h>

using namespace DB;

/// Uses addOverflow method (if available) to avoid UB for sumWithOverflow()
///
/// Since NO_SANITIZE_UNDEFINED works only for the function itself, without
/// callers, and in case of non-POD type (i.e. Decimal) you have overwritten
/// operator+=(), which will have UB.
template <typename T>
struct MyAdd
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(T & lhs, const T & rhs) { lhs += rhs; }
};
template <typename DecimalNativeType>
struct MyAdd<Decimal<DecimalNativeType>>
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(Decimal<DecimalNativeType> & lhs, const Decimal<DecimalNativeType> & rhs)
    {
        lhs.addOverflow(rhs);
    }
};

_Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,bmi2\"))),apply_to=function)")

template <typename T>
struct MySumData
{
    using Impl = MyAdd<T>;
    T sum{};

    /*
    MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
        MULTITARGET_FUNCTION_HEADER(template <typename Value, bool add_if_zero> void NO_SANITIZE_UNDEFINED NO_INLINE),
        addManyConditionalInternalImpl,
        MULTITARGET_FUNCTION_BODY(
            */
            template <typename Value, bool add_if_zero> void NO_SANITIZE_UNDEFINED NO_INLINE
            addManyConditionalInternalImpl
            (const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end) /// NOLINT
            {
                ptr += start;
                condition_map += start;
                size_t count = end - start;
                const auto * end_ptr = ptr + count;

                if constexpr (
                    (is_integer<T> && !is_big_int_v<T>)
                    || (is_decimal<T> && !std::is_same_v<T, Decimal256> && !std::is_same_v<T, Decimal128>))
                {
                    /// For integers we can vectorize the operation if we replace the null check using a multiplication (by 0 for null, 1 for not null)
                    /// https://quick-bench.com/q/MLTnfTvwC2qZFVeWHfOBR3U7a8I
                    T local_sum{};
                    while (ptr < end_ptr)
                    {
                        T multiplier = !*condition_map == add_if_zero;
                        Impl::add(local_sum, *ptr * multiplier);
                        ++ptr;
                        ++condition_map;
                    }
                    Impl::add(sum, local_sum);
                    return;
                }

                if constexpr (std::is_floating_point_v<T>)
                {
                    /// For floating point we use a similar trick as above, except that now we  reinterpret the floating point number as an unsigned
                    /// integer of the same size and use a mask instead (0 to discard, 0xFF..FF to keep)
                    static_assert(sizeof(Value) == 4 || sizeof(Value) == 8);
                    using equivalent_integer = typename std::conditional_t<sizeof(Value) == 4, UInt32, UInt64>;

                    constexpr size_t unroll_count = 128 / sizeof(T);
                    T partial_sums[unroll_count]{};

                    const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

                    while (ptr < unrolled_end)
                    {
                        for (size_t i = 0; i < unroll_count; ++i)
                        {
                            equivalent_integer value;
                            std::memcpy(&value, &ptr[i], sizeof(Value));
                            value &= (!condition_map[i] != add_if_zero) - 1;
                            Value d;
                            std::memcpy(&d, &value, sizeof(Value));
                            Impl::add(partial_sums[i], d);
                        }
                        ptr += unroll_count;
                        condition_map += unroll_count;
                    }

                    for (size_t i = 0; i < unroll_count; ++i)
                        Impl::add(sum, partial_sums[i]);
                }

                T local_sum{};
                while (ptr < end_ptr)
                {
                    if (!*condition_map == add_if_zero)
                        Impl::add(local_sum, *ptr);
                    ++ptr;
                    ++condition_map;
                }
                Impl::add(sum, local_sum);
            } //))

    /// Vectorized version
    template <typename Value, bool add_if_zero>
    void NO_INLINE
    addManyConditionalInternal(const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
    {
        /*
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX512BW))
        {
            addManyConditionalInternalImplAVX512BW<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }

        if (isArchSupported(TargetArch::AVX512F))
        {
            addManyConditionalInternalImplAVX512F<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }

        if (isArchSupported(TargetArch::AVX2))
        {
            addManyConditionalInternalImplAVX2<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }

        if (isArchSupported(TargetArch::SSE42))
        {
            addManyConditionalInternalImplSSE42<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }
#endif
        */
        addManyConditionalInternalImpl<Value, add_if_zero>(ptr, condition_map, start, end);
    }

/*
    MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
        MULTITARGET_FUNCTION_HEADER(template <typename Value, bool add_if_zero> void NO_SANITIZE_UNDEFINED NO_INLINE),
        addManyConditionalInternalImplNew,
        MULTITARGET_FUNCTION_BODY(
            */
            template <typename Value, bool add_if_zero> void NO_SANITIZE_UNDEFINED NO_INLINE
            addManyConditionalInternalImplNew
            (const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end) /// NOLINT
            {
                ptr += start;
                condition_map += start;
                size_t count = end - start;
                const auto * end_ptr = ptr + count;

                if constexpr ((is_integer<T> || is_decimal<T>) && !is_over_big_int<T>)
                {
                    /// For integers we can vectorize the operation if we replace the null check using a multiplication (by 0 for null, 1 for not null)
                    /// https://quick-bench.com/q/MLTnfTvwC2qZFVeWHfOBR3U7a8I
                    T local_sum{};
                    while (ptr < end_ptr)
                    {
                        T multiplier = !*condition_map == add_if_zero;
                        Impl::add(local_sum, *ptr * multiplier);
                        ++ptr;
                        ++condition_map;
                    }
                    Impl::add(sum, local_sum);
                    return;
                }
                else if constexpr (is_integer<T>)
                {
                    T local_sum{};
                    using MaskType = std::conditional_t<sizeof(T) == 16, Int8, Int64>;
                    alignas(64) const MaskType masks[2] = {0, -1};
                    while (ptr < end_ptr)
                    {
                        Value v = *ptr;
                        if constexpr (!add_if_zero)
                            v &= masks[*condition_map];
                        else
                            v &= masks[!*condition_map];

                        Impl::add(local_sum, v);
                        ++ptr;
                        ++condition_map;
                    }
                    Impl::add(sum, local_sum);
                    return;
                }
                else if constexpr (is_decimal<T>)
                {
                    T local_sum{};
                    using MaskType = std::conditional_t<sizeof(T) == 16, Int8, Int64>;
                    alignas(64) const MaskType masks[2] = {0, -1};
                    while (ptr < end_ptr)
                    {
                        Value v = *ptr;
                        if constexpr (!add_if_zero)
                            v.value &= masks[*condition_map];
                        else
                            v.value &= masks[!*condition_map];

                        Impl::add(local_sum, v);
                        ++ptr;
                        ++condition_map;
                    }
                    Impl::add(sum, local_sum);
                    return;
                }
                else if constexpr (std::is_floating_point_v<T>)
                {
                    /// For floating point we use a similar trick as above, except that now we  reinterpret the floating point number as an unsigned
                    /// integer of the same size and use a mask instead (0 to discard, 0xFF..FF to keep)
                    static_assert(sizeof(Value) == 4 || sizeof(Value) == 8);
                    using equivalent_integer = typename std::conditional_t<sizeof(Value) == 4, UInt32, UInt64>;

                    constexpr size_t unroll_count = 128 / sizeof(T);
                    T partial_sums[unroll_count]{};

                    const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

                    while (ptr < unrolled_end)
                    {
                        for (size_t i = 0; i < unroll_count; ++i)
                        {
                            equivalent_integer value;
                            std::memcpy(&value, &ptr[i], sizeof(Value));
                            value &= (!condition_map[i] != add_if_zero) - 1;
                            Value d;
                            std::memcpy(&d, &value, sizeof(Value));
                            Impl::add(partial_sums[i], d);
                        }
                        ptr += unroll_count;
                        condition_map += unroll_count;
                    }

                    for (size_t i = 0; i < unroll_count; ++i)
                        Impl::add(sum, partial_sums[i]);
                }

                T local_sum{};
                while (ptr < end_ptr)
                {
                    Impl::add(local_sum, !*condition_map == add_if_zero ? *ptr : T{});
                    ++ptr;
                    ++condition_map;
                }
                Impl::add(sum, local_sum);
            } //))

    /// Vectorized version
    template <typename Value, bool add_if_zero>
    void NO_INLINE
    addManyConditionalInternalNew(const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
    {
        /*
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX512BW))
        {
            addManyConditionalInternalImplNewAVX512BW<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }

        if (isArchSupported(TargetArch::AVX512F))
        {
            addManyConditionalInternalImplNewAVX512F<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }

        if (isArchSupported(TargetArch::AVX2))
        {
            addManyConditionalInternalImplNewAVX2<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }

        if (isArchSupported(TargetArch::SSE42))
        {
            addManyConditionalInternalImplNewSSE42<Value, add_if_zero>(ptr, condition_map, start, end);
            return;
        }
#endif
        */
        addManyConditionalInternalImplNew<Value, add_if_zero>(ptr, condition_map, start, end);
    }

    template <
        typename Value,
        bool add_if_zero>
    void NO_SANITIZE_UNDEFINED NO_INLINE addManyConditionalInternalImplSIMD(
        const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end) /// NOLINT
    {
        ptr += start;
        condition_map += start;
        size_t count = end - start;
        const auto * end_ptr = ptr + count;

        if constexpr ((is_integer<T> || is_decimal<T>)&&!is_over_big_int<T>)
        {
                    /// For integers we can vectorize the operation if we replace the null check using a multiplication (by 0 for null, 1 for not null)
                    /// https://quick-bench.com/q/MLTnfTvwC2qZFVeWHfOBR3U7a8I
                    T local_sum{};
                    while (ptr < end_ptr)
                    {
                        T multiplier = !*condition_map == add_if_zero;
                        Impl::add(local_sum, *ptr * multiplier);
                        ++ptr;
                        ++condition_map;
                    }
                    Impl::add(sum, local_sum);
                    return;
                }
                else if constexpr (is_integer<T>)
                {
                    T local_sum{};
                    using MaskType = std::conditional_t<sizeof(T) == 16, Int8, Int64>;
                    alignas(64) const MaskType masks[2] = {0, -1};
                    while (ptr < end_ptr)
                    {
                        T value = *ptr;
                        if constexpr (sizeof(T) == 16)
                        {
                            __m128i v = _mm_loadu_si128((__m128i *)&value);
                            __m128i c = _mm_set1_epi8(!*condition_map == add_if_zero);
                            __m128i r = _mm_and_si128(v, c);
                            _mm_storeu_si128((__m128i *)&value, r);
                        }
                        else
                        {
                            __m256i v = _mm256_loadu_si256((__m256i *)&value);
                            __m256i c = _mm256_set1_epi8(!*condition_map == add_if_zero);
                            __m256i r = _mm256_and_si256(v, c);
                            _mm256_storeu_si256((__m256i *)&value, r);
                        }

                        Impl::add(local_sum, value);
                        ++ptr;
                        ++condition_map;
                    }
                    Impl::add(sum, local_sum);
                    return;
                }
                else if constexpr (is_decimal<T>)
                {
                    T local_sum{};
                    using MaskType = std::conditional_t<sizeof(T) == 16, Int8, Int64>;
                    alignas(64) const MaskType masks[2] = {0, -1};
                    while (ptr < end_ptr)
                    {
                        Value v = *ptr;
                        if constexpr (!add_if_zero)
                            v.value &= masks[*condition_map];
                        else
                            v.value &= masks[!*condition_map];

                        Impl::add(local_sum, v);
                        ++ptr;
                        ++condition_map;
                    }
                    Impl::add(sum, local_sum);
                    return;
                }
                else if constexpr (std::is_floating_point_v<T>)
                {
                    /// For floating point we use a similar trick as above, except that now we  reinterpret the floating point number as an unsigned
                    /// integer of the same size and use a mask instead (0 to discard, 0xFF..FF to keep)
                    static_assert(sizeof(Value) == 4 || sizeof(Value) == 8);
                    using equivalent_integer = typename std::conditional_t<sizeof(Value) == 4, UInt32, UInt64>;

                    constexpr size_t unroll_count = 128 / sizeof(T);
                    T partial_sums[unroll_count]{};

                    const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

                    while (ptr < unrolled_end)
                    {
                        for (size_t i = 0; i < unroll_count; ++i)
                        {
                            equivalent_integer value;
                            std::memcpy(&value, &ptr[i], sizeof(Value));
                            value &= (!condition_map[i] != add_if_zero) - 1;
                            Value d;
                            std::memcpy(&d, &value, sizeof(Value));
                            Impl::add(partial_sums[i], d);
                        }
                        ptr += unroll_count;
                        condition_map += unroll_count;
                    }

                    for (size_t i = 0; i < unroll_count; ++i)
                        Impl::add(sum, partial_sums[i]);
                }

                T local_sum{};
                while (ptr < end_ptr)
                {
                    Impl::add(local_sum, !*condition_map == add_if_zero ? *ptr : T{});
                    ++ptr;
                    ++condition_map;
                }
                Impl::add(sum, local_sum);
            }

    template <typename Value, bool add_if_zero>
    void NO_INLINE
    addManyConditionalInternalSIMD(const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
    {
        addManyConditionalInternalImplSIMD<Value, add_if_zero>(ptr, condition_map, start, end);
    }
};

_Pragma("clang attribute pop")

static constexpr size_t ROWS = 65536;

static void initCondition(PaddedPODArray<UInt8> & cond)
{
    cond.resize(ROWS);
    for (size_t i = 0; i < ROWS; ++i)
        cond[i] = std::rand() % 2;
}

template <typename T>
static void initColumn(PaddedPODArray<T> & data)
{
    data.resize(ROWS);
    for (size_t i = 0; i < ROWS; ++i)
        data[i] = static_cast<T>(std::rand());
}

template <typename T>
static void BM_SumWithCondition(benchmark::State & state)
{
    PaddedPODArray<T> data;
    initColumn(data);
    PaddedPODArray<UInt8> cond;
    initCondition(cond);

    for (auto _ : state)
    {
        MySumData<T> sum_data;
        sum_data.template addManyConditionalInternal<T, false>(data.data(), cond.data(), 0, ROWS);
        benchmark::DoNotOptimize(sum_data);
    }
}

template <typename T>
static void BM_SumWithConditionNew(benchmark::State & state)
{
    PaddedPODArray<T> data;
    initColumn(data);
    PaddedPODArray<UInt8> cond;
    initCondition(cond);

    for (auto _ : state)
    {
        MySumData<T> sum_data;
        sum_data.template addManyConditionalInternalNew<T, false>(data.data(), cond.data(), 0, ROWS);
        benchmark::DoNotOptimize(sum_data);
    }
}


template <typename T>
static void BM_SumWithConditionSIMD(benchmark::State & state)
{
    PaddedPODArray<T> data;
    initColumn(data);
    PaddedPODArray<UInt8> cond;
    initCondition(cond);

    for (auto _ : state)
    {
        MySumData<T> sum_data;
        sum_data.template addManyConditionalInternalSIMD<T, false>(data.data(), cond.data(), 0, ROWS);
        benchmark::DoNotOptimize(sum_data);
    }
}

BENCHMARK_TEMPLATE(BM_SumWithCondition, Int64);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, Int64);
BENCHMARK_TEMPLATE(BM_SumWithCondition, UInt64);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, UInt64);
BENCHMARK_TEMPLATE(BM_SumWithCondition, Float64);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, Float64);

BENCHMARK_TEMPLATE(BM_SumWithCondition, Int128);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, Int128);
BENCHMARK_TEMPLATE(BM_SumWithConditionSIMD, Int128);

BENCHMARK_TEMPLATE(BM_SumWithCondition, UInt128);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, UInt128);
BENCHMARK_TEMPLATE(BM_SumWithConditionSIMD, UInt128);

BENCHMARK_TEMPLATE(BM_SumWithCondition, Int256);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, Int256);
BENCHMARK_TEMPLATE(BM_SumWithConditionSIMD, Int256);

BENCHMARK_TEMPLATE(BM_SumWithCondition, UInt256);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, UInt256);
BENCHMARK_TEMPLATE(BM_SumWithConditionSIMD, UInt256);

BENCHMARK_TEMPLATE(BM_SumWithCondition, Decimal32);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, Decimal32);

BENCHMARK_TEMPLATE(BM_SumWithCondition, Decimal64);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, Decimal64);

BENCHMARK_TEMPLATE(BM_SumWithCondition, Decimal128);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, Decimal128);

BENCHMARK_TEMPLATE(BM_SumWithCondition, Decimal256);
BENCHMARK_TEMPLATE(BM_SumWithConditionNew, Decimal256);
