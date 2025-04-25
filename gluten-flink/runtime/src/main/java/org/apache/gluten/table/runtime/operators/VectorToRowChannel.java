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

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.CloseableIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterators;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class VectorToRowChannel {
    private static final Logger LOG = LoggerFactory.getLogger(VectorToRowChannel.class);
    private CloseableIterator<RowVector> inputIterator;
    private transient Output<StreamRecord<RowData>> output;
    private RowType rowType;
    private BufferAllocator allocator;
    private StreamRecord<RowData> outElement = null;

    public VectorToRowChannel(UpIterator  inputIterator,
            RowType rowType,
            Output<StreamRecord<RowData>> output,
            BufferAllocator allocator) {
        this.inputIterator = UpIterators.asJavaIterator(inputIterator);
        this.output = output;
        this.rowType = rowType;
        this.allocator = allocator;
        outElement = new StreamRecord<>(null);
    }

    public void tryFlush() {
        flushInternal(false);
    }

    public void forceFlush() {
        flushInternal(true);
    }

    private void flushInternal(boolean force) {
        while (inputIterator.hasNext(force)) {
            final RowVector rowVector = inputIterator.next();
            List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(rowVector, allocator, rowType);
            for (RowData row : rows) {
                output.collect(outElement.replace(row));
            }
            rowVector.close();
        }
    }

    public void close() throws Exception {
        LOG.debug("close");
        inputIterator.close();
    }

    public void finish() {
        LOG.debug("finish");
        // In case there is still data not flushed
        forceFlush();
    }
}
