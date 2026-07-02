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
package org.apache.gluten.table.runtime.stream.custom;

import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.table.runtime.stream.common.TransformationInspector;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that verifies the Gluten native Kafka source can consume data from a Kafka
 * topic. The test creates a topic, produces JSON records, starts a Flink MiniCluster streaming job
 * that reads from the Kafka source via the Gluten native engine, and verifies: (1) the source and
 * project operators are offloaded to Gluten's native engine (by inspecting the transformation
 * tree), and (2) all produced records are delivered correctly to a JVM collecting sink.
 */
public class KafkaSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceIT.class);
  private static final int KAFKA_PORT = 19093;
  private static final String BOOTSTRAP_SERVERS = "localhost:" + KAFKA_PORT;
  private static final int RECORD_COUNT = 20;

  private static final Set<Integer> RESULTS = ConcurrentHashMap.newKeySet();

  @RegisterExtension
  public static final SharedKafkaTestResource KAFKA =
      new SharedKafkaTestResource()
          .withBrokers(1)
          .registerListener(new PlainListener().onPorts(KAFKA_PORT));

  private static MiniClusterWithClientResource miniCluster;

  @BeforeAll
  static void beforeAll() throws Exception {
    Velox4jEnvironment.initializeOnce();
    miniCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    miniCluster.before();
  }

  @AfterAll
  static void afterAll() {
    if (miniCluster != null) {
      miniCluster.after();
    }
  }

  @Test
  void testKafkaSourceConsumesData() throws Exception {
    RESULTS.clear();

    String topic = "gluten-kafka-source-it-" + UUID.randomUUID();
    String groupId = "gluten-kafka-source-it-group-" + UUID.randomUUID();
    KAFKA.getKafkaTestUtils().createTopic(topic, 1, (short) 1);
    produce(topic, 0, RECORD_COUNT);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    StreamTableEnvironment tableEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());
    tableEnv.executeSql(createKafkaSourceDdl(topic, groupId));

    // Pipeline: Kafka topic -> Gluten native Kafka source -> Gluten native calc (project)
    //     -> TableToDataStream -> JVM map -> JVM collecting sink.
    // toDataStream() triggers exec-node-to-transformation translation, during which Gluten
    // replaces the source and calc with native operators. After this call we can inspect
    // env.getTransformations() to verify the offload.
    Table table = tableEnv.sqlQuery("SELECT id FROM kafka_source");
    DataStream<Row> rows = tableEnv.toDataStream(table);

    // Verify that the source and project are offloaded to Gluten native operators.
    List<Transformation<?>> allTransformations =
        TransformationInspector.collectAll(env.getTransformations());
    StringBuilder treeDesc = new StringBuilder();
    treeDesc.append("Transformation tree (").append(allTransformations.size()).append(" nodes):\n");
    for (int i = 0; i < allTransformations.size(); i++) {
      Transformation<?> t = allTransformations.get(i);
      Object op = TransformationInspector.getOperator(t);
      treeDesc
          .append("  [")
          .append(i)
          .append("] ")
          .append(t.getClass().getSimpleName())
          .append(" -> operator: ")
          .append(op != null ? op.getClass().getName() : "null")
          .append("\n");
    }
    System.err.println(treeDesc);
    // collectAll returns nodes ordered from source (index 0) to sink, matching the
    // natural data-flow direction.
    assertThat(
            TransformationInspector.isOperatorType(
                allTransformations.get(0), GlutenStreamSource.class))
        .as("Pipeline node[0] should be a GlutenStreamSource (native Kafka source)")
        .isTrue();
    assertThat(
            TransformationInspector.isOperatorType(
                allTransformations.get(1), GlutenOneInputOperator.class))
        .as("Pipeline node[1] should be a GlutenOneInputOperator (native calc/project)")
        .isTrue();

    // Verify the native plan JSON content for the source and calc operators.
    String sourcePlan = TransformationInspector.getPlanJson(allTransformations.get(0));
    String calcPlan = TransformationInspector.getPlanJson(allTransformations.get(1));
    System.err.println("Source native plan:\n" + sourcePlan);
    System.err.println("Calc native plan:\n" + calcPlan);
    assertThat(sourcePlan).as("Source plan JSON should not be null").isNotNull();
    assertThat(sourcePlan).contains("TableScanNode");
    assertThat(sourcePlan).contains("KafkaTableHandle");
    assertThat(calcPlan).as("Calc plan JSON should not be null").isNotNull();
    assertThat(calcPlan).contains("ProjectNode");
    assertThat(calcPlan).contains("FieldAccessTypedExpr");
    assertThat(calcPlan).contains("\"fieldName\":\"id\"");

    rows.map(value -> ((Number) value.getField(0)).intValue())
        .name("extract-id")
        .addSink(new CollectingSink())
        .name("collect-results");

    JobClient jobClient = env.executeAsync("gluten-kafka-source-it");
    try {
      waitUntil(
          () -> RESULTS.size() >= RECORD_COUNT, Duration.ofSeconds(60), "all Kafka source records");

      assertThat(RESULTS).containsExactlyInAnyOrderElementsOf(expectedIds());
      LOG.info("All {} records consumed from Kafka source", RECORD_COUNT);
    } finally {
      jobClient.cancel().get(30, TimeUnit.SECONDS);
    }
  }

  private static String createKafkaSourceDdl(String topic, String groupId) {
    return "CREATE TABLE kafka_source ("
        + " id INT,"
        + " payload STRING"
        + ") WITH ("
        + " 'connector' = 'kafka',"
        + " 'topic' = '"
        + topic
        + "',"
        + " 'properties.bootstrap.servers' = '"
        + BOOTSTRAP_SERVERS
        + "',"
        + " 'properties.group.id' = '"
        + groupId
        + "',"
        + " 'scan.startup.mode' = 'earliest-offset',"
        + " 'format' = 'json'"
        + ")";
  }

  private static void produce(String topic, int startInclusive, int endExclusive) throws Exception {
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(kafkaProperties(), new StringSerializer(), new StringSerializer())) {
      for (int id = startInclusive; id < endExclusive; id++) {
        producer.send(
            new ProducerRecord<>(
                topic, Integer.toString(id), "{\"id\":" + id + ",\"payload\":\"v-" + id + "\"}"));
      }
      producer.flush();
    }
  }

  private static Properties kafkaProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
    return properties;
  }

  private static List<Integer> expectedIds() {
    List<Integer> expected = new ArrayList<>();
    for (int id = 0; id < RECORD_COUNT; id++) {
      expected.add(id);
    }
    return expected;
  }

  private static void waitUntil(CheckedBooleanSupplier condition, Duration timeout, String event)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for " + event + ". Current results: " + RESULTS);
  }

  private interface CheckedBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  private static class CollectingSink implements SinkFunction<Integer> {
    @Override
    public void invoke(Integer value, Context context) {
      RESULTS.add(value);
    }
  }
}
