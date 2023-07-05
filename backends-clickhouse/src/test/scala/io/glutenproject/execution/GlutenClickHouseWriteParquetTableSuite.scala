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
package io.glutenproject.execution

import io.glutenproject.GlutenConfig
import io.glutenproject.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import java.io.File

class GlutenClickHouseWriteParquetTableSuite()
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper
  with SharedSparkSession
  with BeforeAndAfterAll {

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def createTPCHNullableTables(): Unit = {}

  override protected def createTPCHNotNullTables(): Unit = {}

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "536870912")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.gluten.sql.columnar.columnartorow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, "/usr/local/clickhouse/lib/libchd.so")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")
      // TODO: support default ANSI policy
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set(
        "spark.sql.warehouse.dir",
        getClass.getResource("/").getPath + "unit-tests-working-home/spark-warehouse")
      .setMaster("local[1]")
  }

  override protected def spark: SparkSession = {
    val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"
    SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
      .getOrCreate()
  }

  private val parquet_table_name = "hive_parquet_test"

  def genTestData(): Seq[AllDataTypesWithComplextType] = {
    (0 to 199).map {
      i =>
        if (i % 100 == 1) {
          AllDataTypesWithComplextType()
        } else {
          AllDataTypesWithComplextType(
            s"$i",
            i,
            i.toLong,
            i.toFloat,
            i.toDouble,
            i.toShort,
            i.toByte,
            i % 2 == 0,
            new java.math.BigDecimal(i + ".56"),
            new java.sql.Date(System.currentTimeMillis()),
            Seq.apply(i + 1, i + 2, i + 3),
            Seq.apply(Option.apply(i + 1), Option.empty, Option.apply(i + 3)),
            Map.apply((i + 1, i + 2), (i + 3, i + 4)),
            Map.empty
          )
        }
    }
  }

  protected def initializeTable(table_name: String, table_create_sql: String): Unit = {
    spark.createDataFrame(genTestData()).createOrReplaceTempView("tmp_t")
    spark.sql(s"drop table IF EXISTS $table_name")
    spark.sql(table_create_sql)
    spark.sql("insert into %s select * from tmp_t".format(table_name))
  }

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    FileUtils.copyDirectory(new File(rootPath + resourcePath), new File(tablesPath))
  }

  def getColumnName(s: String): String = {
    s.replaceAll("\\(", "_").replaceAll("\\)", "_")
  }
  import collection.immutable.ListMap

  def writeAndCheckRead(parquet_table_create_sql: String, fields: Seq[String]): Unit = {
    val originDF = spark.createDataFrame(genTestData())
    originDF.createOrReplaceTempView("origin_table")
    spark.sql(s"drop table IF EXISTS $parquet_table_name")
    spark.sql(parquet_table_create_sql)

    //    spark.sql(
    //      ("insert overwrite local directory '/tmp/destination' stored as parquet select string_field,int_field,long_field,float_field," +
    //        "double_field,short_field,byte_field,bool_field,decimal_field" +
    //        " from tmp_t").format(table_name))

    // test selected column order different from table schema

    // test composite bucket expression

    // test multiple partition col

    val rowsFromOriginTable =
      spark.sql(s"select ${fields.mkString(",")} from origin_table").collect()
    // write them to parquet table
    spark.sql(
      s"insert overwrite $parquet_table_name select ${fields.mkString(",")}" +
        s" from origin_table")

    val dfFromWriteTable =
      spark.sql(
        s"select " +
          s"${fields
              .map(getColumnName)
              .mkString(",")} " +
          s"from $parquet_table_name")
    checkAnswer(dfFromWriteTable, rowsFromOriginTable)
  }

  test("test hive parquet table") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("bool_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      val parquet_table_create_sql =
        s"create table if not exists $parquet_table_name (" +
          fields
            .filterNot(e => e._1.equals("date_field"))
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") +
          " ) partitioned by (date_field date) stored as parquet"

      writeAndCheckRead(parquet_table_create_sql, fields.keys.toSeq)
    }
  }

  test(("test hive parquet table with aggregated results")) {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "false"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("sum(int_field)", "bigint")
      )

      val parquet_table_create_sql =
        s"create table if not exists $parquet_table_name (" +
          fields
            .map(f => s"${getColumnName(f._1)} ${f._2}")
            .mkString(",") +
          " ) stored as parquet"

      writeAndCheckRead(parquet_table_create_sql, fields.keys.toSeq)
    }
  }
}
