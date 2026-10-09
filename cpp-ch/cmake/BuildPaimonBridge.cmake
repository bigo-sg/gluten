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

set(PAIMON_BRIDGE_INSTALL_PREFIX
    "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon-bridge"
    CACHE PATH "Install prefix for libgluten_paimon_bridge.so")
set(PAIMON_BRIDGE_SOURCE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/local-engine/Storages/SubstraitSource/Paimon/Bridge")
set(PAIMON_BRIDGE_BINARY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/paimon-bridge-build")

file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/deps")

ExternalProject_Add(
  paimon_bridge
  SOURCE_DIR ${PAIMON_BRIDGE_SOURCE_DIR}
  BINARY_DIR ${PAIMON_BRIDGE_BINARY_DIR}
  CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${PAIMON_BRIDGE_INSTALL_PREFIX}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DPaimon_DIR=${GLUTEN_PAIMON_CMAKE_DIR}
    -DPAIMON_BUNDLED_DEPS_DIR=${PAIMON_EP_BINARY_DIR}
  BUILD_COMMAND
    ${CMAKE_COMMAND} --build <BINARY_DIR> --parallel && ${CMAKE_COMMAND} --build
    <BINARY_DIR> --target install
  INSTALL_COMMAND ""
  BUILD_BYPRODUCTS "${PAIMON_BRIDGE_BINARY_DIR}/libgluten_paimon_bridge.so"
  DEPENDS paimon_cpp
  USES_TERMINAL_BUILD ON)

# Link libch against the freshly built shared library (install/ prefix can lag behind build/).
set(GLUTEN_PAIMON_BRIDGE_LIB "${PAIMON_BRIDGE_BINARY_DIR}/libgluten_paimon_bridge.so")
