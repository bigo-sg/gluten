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
<<<<<<< HEAD
=======
import io.github.zhztheplayer.velox4j.iterator.CloseableIterator;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
>>>>>>> e1922386b (fix #18, Fetch the next output in non-blocking mode)
import io.github.zhztheplayer.velox4j.iterator.DownIterators;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenSingleInputOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, GlutenOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlutenSingleInputOperator.class);

    private class InputHanldler {
        private BlockingQueue<RowVector> queue;
        private final Session session;
        private final RowType inputType;
        private DownIterator inputIterator;
        private ExternalStream externalStream;
        private List<BoundSplit> splits;
        private PlanNode sourceNode;

        public InputHanldler(Session session, RowType inputType) {
            this.session = session;
            this.inputType = inputType;
            this.queue = new LinkedBlockingQueue<>();

            this.inputIterator = DownIterators.fromBlockingQueue(queue);
            this.externalStream = session.externalStreamOps().bind(inputIterator);
            this.splits = List.of(
                    new BoundSplit(
                            id,
                            -1,
                            new ExternalStreamConnectorSplit("connector-external-stream", externalStream.id())));
            this.sourceNode = new TableScanNode(
                    id,
                    inputType,
                    new ExternalStreamTableHandle("connector-external-stream"),
                    List.of());
        }

        public void addInput(StreamRecord<RowData> element, BufferAllocator allocator) {
            final RowVector rowVector = FlinkRowToVLVectorConvertor.fromRowData(
                    element.getValue(),
                    allocator,
                    this.session,
                    this.inputType);
            queue.add(rowVector);
        }

        public Tuple2<PlanNode, List<BoundSplit>>  getSource() {
            return new Tuple2<>(sourceNode, splits);
        }

        public void finish() {
            inputIterator.finish();
        }

        public void close() {
            externalStream.close();
        }

    };

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
    private InputHanldler inputHanldler;

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
        inputHanldler = new InputHanldler(session, inputType);
        Tuple2<PlanNode, List<BoundSplit>> source = inputHanldler.getSource();
        glutenPlan.setSources(List.of(source.f0));
        LOG.debug("Gluten Plan: {}", Serde.toJson(glutenPlan));
        query = new Query(glutenPlan, source.f1, Config.empty(), ConnectorConfig.empty());
        outputIter = UpIterators.asJavaIterator(session.queryOps().execute(query));
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        inputHanldler.addInput(element, allocator);
        /*
         * There may be cases where all input data has been filtered out. In such scenarios, the
         * process should immediately return and exit to prevent blocking the entire execution flow.
         */
        flushOutput(false);
    }

    private void flushOutput(boolean blocking) {
        while (outputIter.prepareNext(blocking)) {
            final RowVector outRv = outputIter.next();
            List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(
                    outRv,
                    allocator,
                    outputType);
            for (RowData row : rows) {
                output.collect(outElement.replace(row));
            }
            outRv.close();
        }
    }

    @Override
    public void close() throws Exception {
        outputIter.close();
        inputHanldler.close();
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
        inputHanldler.finish();
        flushOutput(true);
    }
}
