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

import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.table.runtime.operators.RowToVectorChannel;
import org.apache.gluten.table.runtime.operators.VectorToRowChannel;
import org.apache.gluten.table.runtime.operators.VeloxExecuteSession;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;


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

    private Query query;
    private RowToVectorChannel inputChannel;
    private VectorToRowChannel outputChannel;
    private VeloxExecuteSession executeSession;

    public GlutenSingleInputOperator(PlanNode plan, String id, RowType inputType, RowType outputType) {
        this.glutenPlan = plan;
        this.id = id;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        executeSession = new VeloxExecuteSession();

        inputChannel = new RowToVectorChannel(executeSession.getSession(),
                inputType,
                executeSession.getAllocator(),
                id);
        Tuple2<PlanNode, List<BoundSplit>> source = inputChannel.getPlanSource();

        glutenPlan.setSources(List.of(source.f0));
        LOG.debug("Gluten Plan: {}", Serde.toJson(glutenPlan));
        query = new Query(glutenPlan, source.f1, Config.empty(), ConnectorConfig.empty());
        outputChannel = new VectorToRowChannel(
                executeSession.execute(query),
                outputType,
                output,
                executeSession.getAllocator());
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        inputChannel.pushOneRow(element);
        /*
         * There may be cases where all input data has been filtered out. In such scenarios, the
         * process should immediately return to prevent blocking the entire execution flow.
         */
        outputChannel.tryFlush();
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close");
        super.close();
        outputChannel.close();
        inputChannel.close();
        executeSession.close();
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

    // finish() typically precedes close().
    @Override
    public void finish() throws Exception {
        LOG.debug("finish");
        super.finish();
        inputChannel.finish();
        outputChannel.finish();
        executeSession.finish();
    }
}
