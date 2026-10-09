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

include(ExternalProject)

set(PAIMON_INSTALL_PREFIX
    "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon"
    CACHE PATH "Install prefix for bundled paimon-cpp")
set(PAIMON_GIT_REPOSITORY
    "https://github.com/apache/paimon-cpp.git"
    CACHE STRING "paimon-cpp git repository")
set(PAIMON_GIT_TAG
    "v0.2.3-rc1"
    CACHE STRING "paimon-cpp git tag (must match find_package version 0.2.3)")

set(PAIMON_EP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon-src")
set(PAIMON_EP_BINARY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon-build")

file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/deps")

ExternalProject_Add(
  paimon_cpp
  GIT_REPOSITORY ${PAIMON_GIT_REPOSITORY}
  GIT_TAG ${PAIMON_GIT_TAG}
  GIT_SHALLOW TRUE
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PAIMON_EP_SOURCE_DIR}
  BINARY_DIR ${PAIMON_EP_BINARY_DIR}
  CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${PAIMON_INSTALL_PREFIX}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_PREFIX_PATH=
    -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF
    -DPAIMON_BUILD_TESTS=OFF
    -DPAIMON_BUILD_SHARED=OFF
    -Dfmt_SOURCE=BUNDLED
    -Dglog_SOURCE=BUNDLED
    -DRapidJSON_SOURCE=BUNDLED
    -DCMAKE_IGNORE_PATH=${CMAKE_CURRENT_SOURCE_DIR}/../ep
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --parallel
  INSTALL_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target install
  USES_TERMINAL_DOWNLOAD ON
  USES_TERMINAL_BUILD ON
  UPDATE_DISCONNECTED TRUE)

set(GLUTEN_PAIMON_CMAKE_DIR "${PAIMON_INSTALL_PREFIX}/lib/cmake/Paimon")
