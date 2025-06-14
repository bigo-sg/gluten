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

package org.apache.gluten.table.runtime.operators;

import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** Gluten legacy source function, call velox plan to execute. */
public class GlutenSourceFunction extends RichParallelSourceFunction<RowData> {

    private final PlanNode planNode;
    private final RowType outputType;
    private final String id;
    private final ConnectorSplit split;
    private volatile boolean isRunning = true;

    public GlutenSourceFunction(
            PlanNode planNode,
            RowType outputType,
            String id,
            ConnectorSplit split) {
        this.planNode = planNode;
        this.outputType = outputType;
        this.id = id;
        this.split = split;
    }

    public PlanNode getPlanNode() {
        return planNode;
    }

    public RowType getOutputType() { return outputType; }

    public String getId() { return id; }

    public ConnectorSplit getConnectorSplit() { return split; }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {

        final List<BoundSplit> splits = List.of(new BoundSplit(id, -1, split));
        final Query query = new Query(planNode, splits, Config.empty(), ConnectorConfig.empty());
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final MemoryManager memoryManager = MemoryManager.create(AllocationListener.NOOP);
        final Session session = Velox4j.newSession(memoryManager);

        UpIterator upIterator = session.queryOps().execute(query);
        while (isRunning) {
            UpIterator.State state = upIterator.advance();
            if (state == UpIterator.State.AVAILABLE) {
                final RowVector outRv = upIterator.get();
                List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(
                        outRv,
                        allocator,
                        outputType);
                for (RowData row : rows) {
                    sourceContext.collect(row);
                }
                outRv.close();
            } else if (state == UpIterator.State.BLOCKED) {
                System.out.println("Get empty row");
            } else {
                System.out.println("Velox task finished");
                break;
            }

        }
        upIterator.close();
        session.close();
        memoryManager.close();
        allocator.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
