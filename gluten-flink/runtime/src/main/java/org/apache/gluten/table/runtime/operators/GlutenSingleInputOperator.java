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

import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.iterator.DownIterators;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.table.runtime.operators.RowToVectorChannel;
import org.apache.gluten.table.runtime.operators.VectorToRowChannel;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.icu.impl.Row;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenSingleInputOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, GlutenOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlutenSingleInputOperator.class);

    private final PlanNode glutenPlan;
    private final String id;
    private final RowType inputType;
    private final RowType outputType;

    private StreamRecord<RowData> outElement = null;

    private MemoryManager memoryManager;
    private Session session;
    private Query query;
    BufferAllocator allocator;
    CloseableIterator<RowVector> outputIter;
    private RowToVectorChannel inputChannel;
    private VectorToRowChannel outputChannel;

    public GlutenSingleInputOperator(PlanNode plan, String id, RowType inputType, RowType outputType) {
        this.glutenPlan = plan;
        this.id = id;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        outElement = new StreamRecord(null);
        memoryManager = MemoryManager.create(AllocationListener.NOOP);
        session = Velox4j.newSession(memoryManager);

        allocator = new RootAllocator(Long.MAX_VALUE);

        inputChannel = new RowToVectorChannel(session, inputType, allocator, id);
        Tuple2<PlanNode, List<BoundSplit>> source = inputChannel.getPlanSource();

        glutenPlan.setSources(List.of(source.f0));
        LOG.debug("Gluten Plan: {}", Serde.toJson(glutenPlan));
        query = new Query(glutenPlan, source.f1, Config.empty(), ConnectorConfig.empty());
        outputChannel = new VectorToRowChannel(session.queryOps().execute(query),
                outputType,
                output,
                allocator);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        inputChannel.pushOneRow(element);
        /*
         * There may be cases where all input data has been filtered out. In such scenarios, the
         * process should immediately return and exit to prevent blocking the entire execution flow.
         */
        outputChannel.tryFlush();
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close");
        outputChannel.close();
        inputChannel.close();
        session.close();
        memoryManager.close();
        allocator.close();
    }

    @Override
    public PlanNode getPlanNode() {
        return glutenPlan;
    }

    @Override
    public RowType getInputType() {
        return inputType;
    }

    @Override
    public RowType getOutputType() {
        return outputType;
    }

    @Override
    public String getId() {
        return id;
    }

    // inish() typically precedes close().
    @Override
    public void finish() {
        LOG.debug("finish");
        inputChannel.finish();
        outputChannel.finish();
    }
}
