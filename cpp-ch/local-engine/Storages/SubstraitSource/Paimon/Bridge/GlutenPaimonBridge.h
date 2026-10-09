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
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowArray;
struct ArrowSchema;

typedef struct GlutenPaimonReaderHandle GlutenPaimonReaderHandle;

enum GlutenPaimonStatus
{
    GLUTEN_PAIMON_OK = 0,
    GLUTEN_PAIMON_EOF = 1,
    GLUTEN_PAIMON_ERROR = -1,
};

GlutenPaimonReaderHandle * glutenPaimonReaderOpen(
    const char * table_path, const uint8_t * split_bytes, size_t split_len, char * err_buf, size_t err_buf_len);

enum GlutenPaimonStatus glutenPaimonReaderNextBatch(
    GlutenPaimonReaderHandle * reader,
    struct ArrowArray * out_array,
    struct ArrowSchema * out_schema,
    char * err_buf,
    size_t err_buf_len);

void glutenPaimonReaderClose(GlutenPaimonReaderHandle * reader);

#ifdef __cplusplus
}
#endif
