# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.20)
project(gluten_paimon_bridge LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

find_package(Paimon 0.2.3 CONFIG REQUIRED)

if(NOT PAIMON_BUNDLED_DEPS_DIR)
  message(FATAL_ERROR "PAIMON_BUNDLED_DEPS_DIR must point to paimon-cpp ExternalProject build dir")
endif()

function(_paimon_static_lib subdir name out_var)
  set(_path "${PAIMON_BUNDLED_DEPS_DIR}/${subdir}/lib/lib${name}.a")
  if(NOT EXISTS "${_path}")
    message(FATAL_ERROR "Missing bundled static library: ${_path}")
  endif()
  set(${out_var} "${_path}" PARENT_SCOPE)
endfunction()

_paimon_static_lib(fmt_ep-install fmt _FMT)
_paimon_static_lib(glog_ep-install glog _GLOG)
_paimon_static_lib(arrow_ep-install parquet _PARQUET)
_paimon_static_lib(arrow_ep-install arrow_dataset _ARROW_DATASET)
_paimon_static_lib(arrow_ep-install arrow_acero _ARROW_ACERO)
_paimon_static_lib(arrow_ep-install arrow _ARROW)
_paimon_static_lib(arrow_ep-install arrow_bundled_dependencies _ARROW_BUNDLED)
_paimon_static_lib(avro_ep-install avrocpp_s _AVRO)
_paimon_static_lib(orc_ep-prefix orc _ORC)
_paimon_static_lib(re2_ep-install re2 _RE2)
_paimon_static_lib(protobuf_ep-install protobuf _PROTOBUF)
_paimon_static_lib(protobuf_ep-install protobuf-lite _PROTO_LITE)
_paimon_static_lib(lz4_ep-install lz4 _LZ4)
_paimon_static_lib(snappy_ep-install snappy _SNAPPY)
_paimon_static_lib(zstd_ep-install zstd _ZSTD)
_paimon_static_lib(zlib_ep-install z _Z)
_paimon_static_lib(tbb_ep-install tbb _TBB)
_paimon_static_lib(tbb_ep-install tbbmalloc _TBB_MALLOC)
set(_ROARING "${PAIMON_BUNDLED_DEPS_DIR}/release/libroaring_bitmap.a")
if(NOT EXISTS "${_ROARING}")
  message(FATAL_ERROR "Missing bundled static library: ${_ROARING}")
endif()
find_library(_THRIFT thrift REQUIRED)
find_library(_UNWIND unwind REQUIRED)
find_library(_UNWIND_ARCH unwind-${CMAKE_SYSTEM_PROCESSOR} REQUIRED)

add_library(gluten_paimon_bridge SHARED GlutenPaimonBridge.cpp)
target_link_options(gluten_paimon_bridge PRIVATE -Wl,-z,defs)

get_target_property(_PAIMON_INCLUDES Paimon::paimon_static INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories(
  gluten_paimon_bridge
  PRIVATE ${_PAIMON_INCLUDES} ${PAIMON_BUNDLED_DEPS_DIR}/arrow_ep-install/include)

target_link_libraries(
  gluten_paimon_bridge
  PRIVATE -Wl,--whole-archive
          Paimon::paimon_local_file_system_static
          Paimon::paimon_parquet_file_format_static
          Paimon::paimon_orc_file_format_static
          Paimon::paimon_avro_file_format_static
          Paimon::paimon_blob_file_format_static
          Paimon::paimon_static
          -Wl,--no-whole-archive
          -Wl,--start-group
          ${_PARQUET}
          ${_ARROW_DATASET}
          ${_ARROW_ACERO}
          ${_ARROW}
          ${_ARROW_BUNDLED}
          ${_AVRO}
          ${_ORC}
          ${_ROARING}
          ${_FMT}
          ${_GLOG}
          ${_RE2}
          ${_PROTOBUF}
          ${_PROTO_LITE}
          ${_LZ4}
          ${_SNAPPY}
          ${_ZSTD}
          ${_Z}
          ${_TBB}
          ${_TBB_MALLOC}
          -Wl,--end-group
          ${_THRIFT}
          ${_UNWIND_ARCH}
          ${_UNWIND}
          dl
          pthread)

if(EXISTS /usr/lib/x86_64-linux-gnu/libstdc++.so.6)
  target_link_libraries(gluten_paimon_bridge PRIVATE /usr/lib/x86_64-linux-gnu/libstdc++.so.6)
endif()

set_target_properties(gluten_paimon_bridge PROPERTIES OUTPUT_NAME gluten_paimon_bridge)

install(TARGETS gluten_paimon_bridge LIBRARY DESTINATION lib)
install(FILES GlutenPaimonBridge.h DESTINATION include)
