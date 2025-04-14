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

package org.apache.gluten.streaming.api.operators;

import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One input operator factory for gluten. */
public class GlutenOneInputOperatorFactory<IN, OUT> extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<IN, OUT>  {
    private static final Logger LOG = LoggerFactory.getLogger(GlutenOneInputOperatorFactory.class);
    private final GlutenOperator operator;

    public GlutenOneInputOperatorFactory(GlutenOperator operator) {
        this.operator = operator;
    }

    public GlutenOperator getOperator() {
        return operator;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        if (operator instanceof TableStreamOperator) {
            TableStreamOperator<OUT> streamOperator = (TableStreamOperator<OUT>) operator;
            streamOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
            streamOperator.setProcessingTimeService(processingTimeService);
            return (T) operator;
        }
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        if (operator instanceof StreamOperator) {
            StreamOperator<?> streamOperator = (StreamOperator) operator;
            return streamOperator.getClass();
        }
        throw new RuntimeException("Not Implemented");
    }
}
