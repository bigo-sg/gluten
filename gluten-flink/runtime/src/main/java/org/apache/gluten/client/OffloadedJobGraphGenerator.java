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
package org.apache.gluten.client;

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.streaming.api.operators.GlutenStreamSourceV2;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperatorV2;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunctionV2;
import org.apache.gluten.table.runtime.typeutils.GlutenRowVectorSerializer;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OffloadedJobGraphGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadedJobGraphGenerator.class);
  private final JobGraph jobGraph;
  private final ClassLoader userClassloader;
  private boolean hasGenerated = false;

  public OffloadedJobGraphGenerator(JobGraph jobGraph, ClassLoader userClassloader) {
    this.jobGraph = jobGraph;
    this.userClassloader = userClassloader;
  }

  public JobGraph generate() {
    if (hasGenerated) {
      throw new IllegalStateException("JobGraph has been generated.");
    }
    hasGenerated = true;
    for (JobVertex jobVertex : jobGraph.getVertices()) {
      offloadJobVertex(jobVertex);
    }
    return jobGraph;
  }

  private void offloadJobVertex(JobVertex jobVertex) {
    OperatorChainSliceGraphGenerator graphGenerator =
        new OperatorChainSliceGraphGenerator(jobVertex, userClassloader);
    OperatorChainSliceGraph chainSliceGraph = graphGenerator.getGraph();
    chainSliceGraph.dumpLog();

    OperatorChainSlice sourceChainSlice = chainSliceGraph.getSourceSlice();
    OperatorChainSliceGraph targetChainSliceGraph = new OperatorChainSliceGraph();
    visitAndFoldChainSlice(sourceChainSlice, chainSliceGraph, targetChainSliceGraph, 0);
    visitAndUpdateStreamEdges(sourceChainSlice, chainSliceGraph, targetChainSliceGraph);
    serializeAllOperatorsConfigs(targetChainSliceGraph);
    targetChainSliceGraph.dumpLog();

    StreamConfig sourceConfig = sourceChainSlice.getOperatorConfigs().get(0);
    StreamConfig targetSourceConfig =
        targetChainSliceGraph.getSlice(sourceChainSlice.id()).getOperatorConfigs().get(0);

    Map<Integer, StreamConfig> chainedConfig = new HashMap<Integer, StreamConfig>();
    if (sourceChainSlice.isOffloadable()) {
      sourceConfig.setStreamOperatorFactory(
          targetSourceConfig.getStreamOperatorFactory(userClassloader));
      List<StreamEdge> chainedOutputs = targetSourceConfig.getChainedOutputs(userClassloader);
      sourceConfig.setChainedOutputs(targetSourceConfig.getChainedOutputs(userClassloader));
      sourceConfig.setTypeSerializerOut(targetSourceConfig.getTypeSerializerOut(userClassloader));
    } else {
      List<StreamConfig> operatorConfigs = sourceChainSlice.getOperatorConfigs();
      for (int i = 0; i < operatorConfigs.size(); i++) {
        StreamConfig opConfig = operatorConfigs.get(i);
        chainedConfig.put(opConfig.getVertexID(), opConfig);
      }
    }
    for (OperatorChainSlice chainSlice : targetChainSliceGraph.getSlices().values()) {
      if (chainSlice.id().equals(sourceChainSlice.id())) {
        continue;
      }
      List<StreamConfig> operatorConfigs = chainSlice.getOperatorConfigs();
      for (StreamConfig opConfig : operatorConfigs) {
        chainedConfig.put(opConfig.getVertexID(), opConfig);
      }
    }
    sourceConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedConfig);
    sourceConfig.serializeAllConfigs();
  }

  // Fold offloadable operator chain slice
  private void visitAndFoldChainSlice(
      OperatorChainSlice chainSlice,
      OperatorChainSliceGraph originalChainSliceGraph,
      OperatorChainSliceGraph targetChainSliceGraph,
      Integer chainedIndex) {
    List<Integer> outputs = chainSlice.getOutputs();
    List<Integer> outputIndex = new ArrayList<>();
    OperatorChainSlice resultChainSlice = null;
    if (chainSlice.isOffloadable()) {
      resultChainSlice =
          foldOffloadableOperatorChainSlice(originalChainSliceGraph, chainSlice, chainedIndex);
      chainedIndex = chainedIndex + 1;
    } else {
      resultChainSlice = foldUnoffloadableOperatorChainSlice(chainSlice, chainedIndex);
      chainedIndex = chainedIndex + chainSlice.getOperatorConfigs().size();
    }

    resultChainSlice.getInputs().addAll(chainSlice.getInputs());
    resultChainSlice.getOutputs().addAll(chainSlice.getOutputs());
    targetChainSliceGraph.addSlice(chainSlice.id(), resultChainSlice);

    for (Integer outputChainIndex : outputs) {
      OperatorChainSlice outputChainSlice = originalChainSliceGraph.getSlice(outputChainIndex);
      OperatorChainSlice outputResultChainSlice = targetChainSliceGraph.getSlice(outputChainIndex);
      if (outputResultChainSlice == null) {
        visitAndFoldChainSlice(
            outputChainSlice, originalChainSliceGraph, targetChainSliceGraph, chainedIndex);
      }
    }
  }

  private OperatorChainSlice foldUnoffloadableOperatorChainSlice(
      OperatorChainSlice originalChainSlice, Integer chainedIndex) {
    OperatorChainSlice resultChainSlice = new OperatorChainSlice(originalChainSlice.id());
    List<StreamConfig> operatorConfigs = originalChainSlice.getOperatorConfigs();
    for (StreamConfig opConfig : operatorConfigs) {
      StreamConfig newOpConfig = new StreamConfig(new Configuration(opConfig.getConfiguration()));
      newOpConfig.setChainIndex(chainedIndex);
      resultChainSlice.getOperatorConfigs().add(newOpConfig);
    }
    resultChainSlice.setOffloadable(false);
    return resultChainSlice;
  }

  private OperatorChainSlice foldOffloadableOperatorChainSlice(
      OperatorChainSliceGraph chainSliceGraph,
      OperatorChainSlice originalChainSlice,
      Integer chainedIndex) {
    OperatorChainSlice resultChainSlice = new OperatorChainSlice(originalChainSlice.id());
    List<StreamConfig> operatorConfigs = originalChainSlice.getOperatorConfigs();

    // Put all operators into a single velox plan.
    PlanNode currentPlanNode = null;
    PlanNode rootPlanNode = null;
    for (int i = operatorConfigs.size() - 1; i >= 0; i--) {
      StreamConfig opConfig = operatorConfigs.get(i);
      GlutenOperator op = getGlutenOperator(opConfig).get();
      PlanNode nextPlanNode = op.getPlanNodeV2();
      if (currentPlanNode != null) {
        currentPlanNode.setSources(List.of(nextPlanNode));
      }
      if (rootPlanNode == null) {
        rootPlanNode = currentPlanNode;
      }
      currentPlanNode = nextPlanNode;
    }
    // There is only one operator in this operator chain slice.
    if (rootPlanNode == null) {
      rootPlanNode = getGlutenOperator(operatorConfigs.get(0)).get().getPlanNodeV2();
    }

    StreamConfig sourceConfig = operatorConfigs.get(0);
    GlutenOperator sourceOp = getGlutenOperator(sourceConfig).get();
    StreamConfig rootOpConfig = operatorConfigs.get(operatorConfigs.size() - 1);
    GlutenOperator rootOp = getGlutenOperator(rootOpConfig).get();
    StreamConfig resultOpConfig =
        new StreamConfig(new Configuration(sourceConfig.getConfiguration()));

    if (sourceOp instanceof GlutenStreamSourceV2) {
      boolean couldOutputRowVector = couldOutputRowVector(originalChainSlice, chainSliceGraph);
      Class<?> outClass = couldOutputRowVector ? RowVector.class : RowData.class;
      GlutenStreamSourceV2 newSourceOp =
          new GlutenStreamSourceV2(
              new GlutenSourceFunctionV2<>(
                  rootPlanNode,
                  rootOp.getOutputTypes(),
                  sourceOp.getId(),
                  ((GlutenStreamSourceV2) sourceOp).getConnectorSplit(),
                  outClass));
      resultOpConfig.setStreamOperator(newSourceOp);
      if (couldOutputRowVector) {
        RowType rowType = rootOp.getOutputTypes().entrySet().iterator().next().getValue();
        resultOpConfig.setTypeSerializerOut(new GlutenRowVectorSerializer(rowType));
      }

    } else if (sourceOp instanceof GlutenOneInputOperatorV2) {
      boolean couldOutputRowVector = couldOutputRowVector(originalChainSlice, chainSliceGraph);
      boolean couldInputRowVector = couldInputRowVector(originalChainSlice, chainSliceGraph);
      Class<?> inClass = couldInputRowVector ? RowVector.class : RowData.class;
      Class<?> outClass = couldOutputRowVector ? RowVector.class : RowData.class;
      GlutenOneInputOperatorV2 newOneInputOp =
          new GlutenOneInputOperatorV2(
              rootPlanNode,
              sourceOp.getId(),
              sourceOp.getInputType(),
              rootOp.getOutputTypes(),
              inClass,
              outClass);
      resultOpConfig.setStreamOperator(newOneInputOp);
      if (couldOutputRowVector) {
        RowType rowType = rootOp.getOutputTypes().entrySet().iterator().next().getValue();
        resultOpConfig.setTypeSerializerOut(new GlutenRowVectorSerializer(rowType));
      }
      if (couldInputRowVector) {
        resultOpConfig.setupNetworkInputs(new GlutenRowVectorSerializer(sourceOp.getInputType()));
      }
    } else {
      throw new UnsupportedOperationException(
          "Only GlutenStreamSourceV2 could be the root operator of an offloaded operator chain slice.");
    }

    resultOpConfig.setChainIndex(chainedIndex);
    resultChainSlice.getOperatorConfigs().add(resultOpConfig);
    resultChainSlice.setOffloadable(true);
    return resultChainSlice;
  }

  private StreamNode mockStreamNode(StreamConfig streamConfig) {
    return new StreamNode(
        streamConfig.getVertexID(),
        null,
        null,
        (StreamOperatorFactory<?>) streamConfig.getStreamOperatorFactory(userClassloader),
        streamConfig.getOperatorName(),
        null);
  }

  private void visitAndUpdateStreamEdges(
      OperatorChainSlice originalChainSlice,
      OperatorChainSliceGraph originalChainSliceGraph,
      OperatorChainSliceGraph targetChainSliceGraph) {
    OperatorChainSlice targetChainSlice = targetChainSliceGraph.getSlice(originalChainSlice.id());
    if (targetChainSlice.isOffloadable()) {
      List<Integer> outputIDs = originalChainSlice.getOutputs();
      List<StreamConfig> operatorConfigs = targetChainSlice.getOperatorConfigs();
      StreamConfig targetOpConfig = operatorConfigs.get(0);
      if (outputIDs.size() == 0) {
        targetOpConfig.setChainedOutputs(new ArrayList<>());
        return;
      }
      List<StreamEdge> newOutputEdges = new ArrayList<>();
      List<StreamEdge> originalOutputEdges =
          originalChainSlice
              .getOperatorConfigs()
              .get(originalChainSlice.getOperatorConfigs().size() - 1)
              .getChainedOutputs(userClassloader);
      for (int i = 0; i < outputIDs.size(); i++) {
        Integer outputID = outputIDs.get(i);
        OperatorChainSlice outputOriginalChainSlice = originalChainSliceGraph.getSlice(outputID);
        OperatorChainSlice outputTargetChainSlice = targetChainSliceGraph.getSlice(outputID);
        StreamConfig outputOpConfig =
            outputTargetChainSlice
                .getOperatorConfigs()
                .get(0); // The first operator config is the representative.
        StreamEdge originalEdge = originalOutputEdges.get(i);
        StreamEdge newEdge =
            new StreamEdge(
                mockStreamNode(targetOpConfig),
                mockStreamNode(outputOpConfig),
                originalEdge.getTypeNumber(),
                originalEdge.getBufferTimeout(),
                originalEdge.getPartitioner(),
                originalEdge.getOutputTag(),
                originalEdge.getExchangeMode(),
                0,
                originalEdge.getIntermediateDatasetIdToProduce());
        newOutputEdges.add(newEdge);
      }
      targetOpConfig.setChainedOutputs(newOutputEdges);
    }

    for (Integer outputChain : originalChainSlice.getOutputs()) {
      visitAndUpdateStreamEdges(
          originalChainSliceGraph.getSlice(outputChain),
          originalChainSliceGraph,
          targetChainSliceGraph);
    }
  }

  void serializeAllOperatorsConfigs(OperatorChainSliceGraph chainSliceGraph) {
    for (OperatorChainSlice chainSlice : chainSliceGraph.getSlices().values()) {
      List<StreamConfig> operatorConfigs = chainSlice.getOperatorConfigs();
      for (StreamConfig opConfig : operatorConfigs) {
        opConfig.serializeAllConfigs();
      }
    }
  }

  private Optional<GlutenOperator> getGlutenOperator(StreamConfig taskConfig) {
    StreamOperatorFactory operatorFactory = taskConfig.getStreamOperatorFactory(userClassloader);
    if (operatorFactory instanceof SimpleOperatorFactory) {
      StreamOperator streamOperator = taskConfig.getStreamOperator(userClassloader);
      if (streamOperator instanceof GlutenOperator) {
        return Optional.of((GlutenOperator) streamOperator);
      }
    } else if (operatorFactory instanceof GlutenOneInputOperatorFactory) {
      return Optional.of(((GlutenOneInputOperatorFactory) operatorFactory).getOperator());
    }
    return Optional.empty();
  }

  boolean isAllOffloadable(OperatorChainSliceGraph chainSliceGraph, List<Integer> chainIDs) {
    for (Integer chainID : chainIDs) {
      OperatorChainSlice chainSlice = chainSliceGraph.getSlice(chainID);
      if (!chainSlice.isOffloadable()) {
        return false;
      }
    }
    return true;
  }

  boolean couldOutputRowVector(
      OperatorChainSlice chainSlice, OperatorChainSliceGraph chainSliceGraph) {
    boolean could = true;
    for (Integer outputID : chainSlice.getOutputs()) {
      OperatorChainSlice outputChainSlice = chainSliceGraph.getSlice(outputID);
      if (!outputChainSlice.isOffloadable()) {
        could = false;
        break;
      }
      List<Integer> inputs = outputChainSlice.getInputs();
      if (!isAllOffloadable(chainSliceGraph, inputs)) {
        could = false;
        break;
      }
    }
    return could;
  }

  boolean couldInputRowVector(
      OperatorChainSlice chainSlice, OperatorChainSliceGraph chainSliceGraph) {
    boolean could = true;
    for (Integer inputID : chainSlice.getInputs()) {
      OperatorChainSlice inputChainSlice = chainSliceGraph.getSlice(inputID);
      if (!inputChainSlice.isOffloadable()) {
        could = false;
        break;
      }
      if (!couldOutputRowVector(inputChainSlice, chainSliceGraph)) {
        could = false;
        break;
      }
    }
    return could;
  }
}
