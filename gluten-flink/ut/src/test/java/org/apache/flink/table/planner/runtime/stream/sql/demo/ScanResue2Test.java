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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.planner.runtime.stream.GlutenStreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/*
 * This is a demo test. 
 */
class ScanReuse2Test extends GlutenStreamingTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(ScanReuse2Test.class);

    @Override
    @BeforeEach
    public void before() throws Exception {
        super.before();
        List<Row> rows =
                Arrays.asList(Row.of(1, 1L, "1"), Row.of(2, 2L, "2"), Row.of(3, 3L, "3"));
        createSimpleBoundedValuesTable("MyTable", "a int, b bigint, c string", rows);
    }

    @Test
    void testFilter() {
        String query = "select a, b,c from MyTable where a > 1";
        runAndCheck(query, Arrays.asList("+I[2, 2, 2]", "+I[3, 3, 3]"));
    }
}
