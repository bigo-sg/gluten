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
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.DownIterators;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RowToVectorChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RowToVectorChannel.class);
    private final Session session;
    private final RowType rowType;
    private BufferAllocator allocator;

    private BlockingQueue<RowVector> queue;
    private DownIterator inputIterator;

    private ExternalStream externalStream;
    private List<BoundSplit> splits;
    private PlanNode sourceNode;

    public RowToVectorChannel(Session session, RowType rowType, BufferAllocator allocator, String id) {
        this.session = session;
        this.rowType = rowType;
        this.allocator = allocator;

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
                rowType,
                new ExternalStreamTableHandle("connector-external-stream"),
                List.of());
    }

    Tuple2<PlanNode, List<BoundSplit>> getPlanSource() {
        return new Tuple2<>(sourceNode, splits);
    }

    public void pushOneRow(StreamRecord<RowData> element) {
        final RowVector rowVector = FlinkRowToVLVectorConvertor.fromRowData(
                element.getValue(),
                allocator,
                this.session,
                this.rowType);
        queue.add(rowVector);
    }

    public void finish() {
        LOG.debug("finish");
        inputIterator.finish();
    }

    public void close() throws Exception {
        LOG.debug("close");
        externalStream.close();
    }
}
