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
package org.apache.gluten.execution

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.{PaimonLocalFilesBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.paimon.io.DataOutputSerializer
import org.apache.paimon.spark.PaimonInputPartition
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit

import scala.collection.JavaConverters._

case class CHPaimonScanTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    override val pushDownFilters: Option[Seq[Expression]] = None,
    allowPaimonCppPkRead: Boolean = false)
  extends PaimonScanTransformer(
    output,
    scan,
    runtimeFilters,
    table,
    keyGroupedPartitioning,
    commonPartitionValues,
    pushDownFilters) {

  override protected def copyWith(
      output: Seq[AttributeReference],
      runtimeFilters: Seq[Expression],
      pushDownFilters: Option[Seq[Expression]]): PaimonScanTransformer =
    copy(
      output = output,
      runtimeFilters = runtimeFilters,
      pushDownFilters = pushDownFilters)

  override protected def validateFileStoreTable(table: FileStoreTable): Option[String] = {
    // A zero-column scan can reach Paimon's table-schema lookup through the virtual column.
    if (output.isEmpty || getDataSchema.fields.isEmpty) {
      Some("Native Paimon read requires a data column.")
    } else if (!allowPaimonCppPkRead && !table.primaryKeys().isEmpty) {
      Some("Not support Paimon PK table.")
    } else {
      None
    }
  }

  override def getSplitInfosFromPartitions(
      partitions: Seq[(Partition, ReadFileFormat)]): Seq[SplitInfo] = {
    partitions.map { case (partition, _) =>
      partition match {
        case p: SparkDataSourceRDDPartition =>
          val splits = p.inputPartitions.flatMap {
            case input: PaimonInputPartition =>
              input.splits.map {
                case split: DataSplit =>
                  val out = new DataOutputSerializer(1024)
                  split.serialize(out)
                  out.getCopyOfBuffer
                case other =>
                  throw new GlutenNotSupportException(s"Unsupported Paimon split: $other")
              }
            case other =>
              throw new GlutenNotSupportException(s"Unsupported input partition type: $other")
          }
          val node = PaimonLocalFilesBuilder.makeNativePaimonLocalFiles(
            p.index,
            coreOptions.path().toString,
            splits.asJava,
            p.preferredLocations().toList.asJava)
          node.setFileSchema(getDataSchema)
          node
        case _ => throw new GlutenNotSupportException()
      }
    }
  }
}

object CHPaimonScanTransformer {
  def apply(
      batchScan: BatchScanExec,
      allowPaimonCppPkRead: Boolean): CHPaimonScanTransformer = {
    new CHPaimonScanTransformer(
      batchScan.output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = batchScan.table,
      keyGroupedPartitioning = batchScan.keyGroupedPartitioning,
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan),
      allowPaimonCppPkRead = allowPaimonCppPkRead)
  }
}
