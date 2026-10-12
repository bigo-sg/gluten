#!/usr/bin/env bash
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

set -euo pipefail

paimon_lib=${1:?Usage: paimon-bash-preload-test.sh /path/to/libpaimon.so [/path/to/libch.so]}

undefined_symbols=$(nm -D --undefined-only "$paimon_lib")
if rg -q '[[:space:]]U[[:space:]](malloc|free|calloc|realloc|memalign|aligned_alloc|posix_memalign)(@.*)?$' <<< "$undefined_symbols"; then
  echo 'libpaimon.so still imports a preemptible allocator function' >&2
  exit 1
fi

env LD_PRELOAD="$paimon_lib" /bin/bash -c ':'

if [[ $# -ge 2 ]]; then
  env LD_PRELOAD="$2:$paimon_lib" /bin/bash -c ':'
fi
