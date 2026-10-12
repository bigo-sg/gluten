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

: "${SPARK_HOME:?Set SPARK_HOME to the Spark installation containing the Gluten JAR}"
: "${PAIMON_HDFS_WAREHOUSE:?Set PAIMON_HDFS_WAREHOUSE to an hdfs:// warehouse}"
: "${PAIMON_HDFS_TABLE:?Set PAIMON_HDFS_TABLE to an existing Paimon table}"
: "${PAIMON_HDFS_COLUMN:?Set PAIMON_HDFS_COLUMN to a non-partition data column}"
: "${PAIMON_HDFS_SITE_XML:?Set PAIMON_HDFS_SITE_XML to the HDFS configuration XML}"
: "${GLUTEN_CH_LIBPATH:?Set GLUTEN_CH_LIBPATH to the rebuilt libch.so}"

[[ "$PAIMON_HDFS_WAREHOUSE" == hdfs://* ]]
[[ "$PAIMON_HDFS_TABLE" =~ ^[a-zA-Z_][a-zA-Z_0-9.]*$ ]]
[[ "$PAIMON_HDFS_COLUMN" =~ ^[a-zA-Z_][a-zA-Z_0-9]*$ ]]
if [[ -n "${PAIMON_HDFS_FILTER_COLUMN:-}" ]]; then
  [[ "$PAIMON_HDFS_FILTER_COLUMN" =~ ^[a-zA-Z_][a-zA-Z_0-9]*$ ]]
  [[ "${PAIMON_HDFS_FILTER_VALUE:-}" =~ ^[a-zA-Z_0-9-]+$ ]]
  filter=" WHERE $PAIMON_HDFS_FILTER_COLUMN = '$PAIMON_HDFS_FILTER_VALUE'"
else
  filter=""
fi

test_log=$(mktemp)
finish() {
  status=$?
  if [[ $status -ne 0 ]]; then
    tail -60 "$test_log" >&2
  fi
  rm -f "$test_log"
}
trap finish EXIT

LD_PRELOAD="$GLUTEN_CH_LIBPATH" "$SPARK_HOME/bin/spark-sql" \
  --master 'local[1]' \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.gluten.enabled=true \
  --conf spark.gluten.sql.enable.native.validation=false \
  --conf spark.gluten.sql.columnar.backend.lib=ch \
  --conf spark.gluten.sql.columnar.libpath="$GLUTEN_CH_LIBPATH" \
  --conf spark.gluten.sql.columnar.backend.ch.paimon.native.read.enabled=true \
  --conf spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level=debug \
  --conf spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.libhdfs3_conf="$PAIMON_HDFS_SITE_XML" \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse="$PAIMON_HDFS_WAREHOUSE" \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.adaptive.enabled=false \
  -e "SELECT $PAIMON_HDFS_COLUMN FROM $PAIMON_HDFS_TABLE$filter LIMIT 5; SELECT COUNT($PAIMON_HDFS_COLUMN) FROM $PAIMON_HDFS_TABLE$filter; SELECT COUNT(*) FROM $PAIMON_HDFS_TABLE; SELECT SUM(1) FROM $PAIMON_HDFS_TABLE$filter; SELECT COUNT(1) FROM $PAIMON_HDFS_TABLE$filter" >"$test_log" 2>&1

rg -q 'PaimonReader: Opening native Paimon reader: table_path=hdfs://' "$test_log"
rg -q 'PaimonReader: Native Paimon reader emitted batch: table_path=hdfs://' "$test_log"
rg -m 6 'PaimonReader: (Opening native Paimon reader|Native Paimon reader emitted batch)' "$test_log"
