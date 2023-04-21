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

import io.glutenproject.expression._
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{LocalFilesBuilder, RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode, Average, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.Exchange

import com.google.protobuf.Any
import io.substrait.proto.SortField

import java.util
import java.util.Locale

/*
 * How to test SortAggregateExec:
 * - spark.sql.execution.useObjectHashAggregateExec=false
 * - spark.sql.autoBroadcastJoinThreshold=-1
 * - query: select k1, collect_list(k1) as k3 from (select floor(id/4) as k1 from (select id from range(10)) as t1) as t2 group by k1
 */
object CHAggregateExecTransformer {
  def getAggregateResultAttributes(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression]): Seq[Attribute] = {
    val groupingAttributes = groupingExpressions.map(
      expression => { ConverterUtils.getAttrFromExpr(expression).toAttribute })
    val aggregateAttributes = aggregateExpressions.map(expression => { expression.resultAttribute })
    groupingAttributes ++ aggregateAttributes
  }

  def genAggregatePartialResultColumnName(attr: Attribute): String = {
    ConverterUtils.genColumnNameWithExprId(attr) + "#Partial#" + ConverterUtils
      .getShortAttributeName(attr)
  }

  def genColumnTypeNode(
      columnName: String,
      attribute: Attribute,
      aggregateExpressions: Seq[AggregateExpression]): TypeNode = {
    val aggregateExpression =
      aggregateExpressions.find(expression => { expression.resultAttribute == attribute })
    val (dataType, nullable) = if (!aggregateExpression.isDefined) {
      (attribute.dataType, attribute.nullable)
    } else {
      val lowercaseName = columnName.toLowerCase(Locale.ROOT)
      if (lowercaseName.startsWith("avg#")) {
        val dataType = aggregateExpression.get match {
          case aggExpr: AggregateExpression =>
            aggExpr.aggregateFunction match {
              case avg: Average => avg.child.dataType
              case _ => attribute.dataType
            }
          case _ => attribute.dataType
        }
        (dataType, attribute.nullable)
      } else if (lowercaseName.startsWith("collect_list#")) {
        aggregateExpression.get match {
          case aggExpr: AggregateExpression =>
            aggExpr.aggregateFunction match {
              case collectList: CollectList =>
                (collectList.child.dataType, collectList.child.nullable)
              case _ => (attribute.dataType, attribute.nullable)
            }
          case _ => (attribute.dataType, attribute.nullable)
        }
      } else {
        (attribute.dataType, attribute.nullable)
      }
    }
    ConverterUtils.getTypeNode(dataType, nullable)
  }
}

case class CHAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {
  lazy val aggregateResultAttributes =
    CHAggregateExecTransformer.getAggregateResultAttributes(
      groupingExpressions,
      aggregateExpressions)

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childContext = child match {
      case transform: TransformSupport => transform.doTransform(context)
      case _ => null
    }
    logDebug(s"xxx CHAggregateExecTransformer xxxx")
    val aggregationParams = new AggregationParams
    val operatorId = context.nextOperatorId
    // For hash aggregate,  the final merge aggregate operator's upstream is an exchange node,
    // so the child context is null. But for sort aggregate, its upsteam is a sort node, and
    // child context is not null (Is there any case that the sort node fallback into jvm?).
    val (aggregateRel, inputAttributes, outputAttributes) = if (childContext != null) {
      // The final HashAggregateExecTransformer and partial HashAggregateExecTransformer
      // are in the one WholeStageTransformer.
      if (
        child.isInstanceOf[CHAggregateExecTransformer] &&
        childContext.outputAttributes == aggregateResultAttributes
      ) {
        (
          getAggRel(context, operatorId, aggregationParams, childContext.root),
          childContext.outputAttributes,
          output)
      } else {
        (
          getAggRel(context, operatorId, aggregationParams, childContext.root),
          childContext.outputAttributes,
          aggregateResultAttributes)
      }
    } else {
      // The final stage of the aggregate operator.
      // typeNodes and names are used to create the input schema of the aggregate operator.
      val typeNodes = new util.ArrayList[TypeNode]
      val names = new util.ArrayList[String]
      if (
        (child.find(_.isInstanceOf[Exchange]).isEmpty &&
          child.find(_.isInstanceOf[QueryStageExec]).isEmpty) ||
        (child.isInstanceOf[InputAdapter] &&
          child.asInstanceOf[InputAdapter].child.isInstanceOf[UnionExecTransformer])
      ) {
        logDebug(
          s"xxxx child.output:${child.output}\n"
            + s"aggregateResultAttributes:$aggregateResultAttributes")
        // It'is the first stage of the aggregate operator?
        for (attr <- child.output) {
          typeNodes.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
          names.add(ConverterUtils.genColumnNameWithExprId(attr))
        }
        (child.output, aggregateResultAttributes)
      } else {
        logDebug(
          s"xxx aggregateResultAttributes:$aggregateResultAttributes\n" +
            s"aggregateAttributes:$aggregateAttributes")
        for (attr <- aggregateResultAttributes) {
          val colName = if (aggregateAttributes.contains(attr)) {
            // The aggregate attribute is the partial result of the aggregate operator.
            // column is named in special format
            CHAggregateExecTransformer.genAggregatePartialResultColumnName(attr)
          } else {
            // namely the grouping columns
            ConverterUtils.genColumnNameWithExprId(attr)
          }
          names.add(colName)
          typeNodes.add(
            CHAggregateExecTransformer.genColumnTypeNode(colName, attr, aggregateExpressions))
        }
      }
      val blockIteratorIndex = context.nextIteratorIndex
      context.setIteratorNode(
        blockIteratorIndex,
        LocalFilesBuilder.makeLocalFiles(
          ConverterUtils.ITERATOR_PREFIX.concat(blockIteratorIndex.toString.toString)))
      val readRel =
        RelBuilder.makeReadRel(typeNodes, names, null, blockIteratorIndex, context, operatorId);
      (
        getAggRel(context, operatorId, aggregationParams, readRel),
        aggregateResultAttributes,
        output
      )
    }
    TransformContext(inputAttributes, outputAttributes, aggregateRel)
  }

  override def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggregationParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode = {
    logDebug(s"xxx gluten . needsPreProjection:$needsPreProjection")
    val originalInputAttributes = child.output
    val aggRel = if (needsPreProjection) {
      getAggRelWithPreProjection(context, originalInputAttributes, operatorId, input, validation)
    } else {
      getAggRelWithoutPreProjection(
        context,
        aggregateResultAttributes,
        operatorId,
        input,
        validation)
    }
    if (!needsPostProjection(allAggregateResultAttributes)) {
      aggRel
    } else {
      applyPostProjection(context, aggRel, operatorId, validation)
    }
  }

  override def getAggRelWithoutPreProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val registeredFunctions = context.registeredFunction

    // Validation check
    val aggregateExpressionModes = aggregateExpressions.map(_.mode).distinct
    if (
      aggregateExpressionModes.contains(PartialMerge) && aggregateExpressionModes.contains(Final)
    ) {
      throw new IllegalStateException("PartialMerge co-exists with Fina")
    }

    val groupingExprNodes = new util.ArrayList[ExpressionNode]
    groupingExpressions.foreach(
      expr => {
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(expr, child.output)
          .doTransform(registeredFunctions)
        groupingExprNodes.add(exprNode)
      })

    val aggregateFitlers = new util.ArrayList[ExpressionNode]
    aggregateExpressions.foreach(
      aggExpr => {
        logDebug(s"xxx gluten. aggregateFitlers, aggExpr:$aggExpr")
        if (aggExpr.filter.isDefined) {
          aggregateFitlers.add(
            ExpressionConverter
              .replaceWithExpressionTransformer(aggExpr.filter.get, child.output)
              .doTransform(registeredFunctions))
        } else {
          aggregateFitlers.add(null)
        }
      })

    val sortFields = genSortFields(context, child, originalInputAttributes)

    val aggregateFunctions =
      genAggregateFunctions(context, aggregateExpressionModes, sortFields, originalInputAttributes)

    if (!validation) {
      RelBuilder.makeAggregateRel(
        input,
        groupingExprNodes,
        aggregateFunctions,
        aggregateFitlers,
        context,
        operatorId)
    } else {
      val inputTypeNodes = new util.ArrayList[TypeNode]
      for (attr <- originalInputAttributes) {
        inputTypeNodes.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodes).toProtobuf))
      RelBuilder.makeAggregateRel(
        input,
        groupingExprNodes,
        aggregateFunctions,
        aggregateFitlers,
        extensionNode,
        context,
        operatorId)
    }
  }

  def genAggregateFunctions(
      context: SubstraitContext,
      distinctModes: Seq[AggregateMode],
      sortFields: util.ArrayList[SortField],
      inputAttributes: Seq[Attribute]): util.ArrayList[AggregateFunctionNode] = {
    val registeredFunctions = context.registeredFunction
    val aggregateFunctions = new util.ArrayList[AggregateFunctionNode]
    val containsPartialMode = distinctModes.contains(Partial)
    aggregateExpressions.foreach(
      aggExpr => {
        logDebug(s"xxx gluten. genAggregateFunctions, aggExpr:$aggExpr")
        val aggregateFunction = aggExpr.aggregateFunction
        val childrenExprNodesList = new util.ArrayList[ExpressionNode]
        val childrenExprNodes = aggExpr.mode match {
          case Partial =>
            aggregateFunction.children.toList.map(
              expr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(expr, child.output)
                  .doTransform(registeredFunctions)
              })
          case PartialMerge if containsPartialMode =>
            // this is the case where PartialMerge co-exists with Partial
            // so far, it only happens in a three-stage count distinct case
            // e.g. select sum(a), count(distinct b) from f
            if (!child.isInstanceOf[BaseAggregateExec]) {
              throw new IllegalStateException(
                "PartialMerge co-exists with Partial, but child is not HashAggregateExec")
            }
            val childAgg = child.asInstanceOf[BaseAggregateExec]
            val aggTypesExpr = ExpressionConverter.replaceWithExpressionTransformer(
              aggExpr.resultAttribute,
              CHAggregateExecTransformer.getAggregateResultAttributes(
                childAgg.groupingExpressions,
                childAgg.aggregateExpressions))
            Seq(aggTypesExpr.doTransform(registeredFunctions))
          case Final | PartialMerge =>
            Seq(
              ExpressionConverter
                .replaceWithExpressionTransformer(aggExpr.resultAttribute, inputAttributes)
                .doTransform(registeredFunctions))
          case other =>
            throw new UnsupportedOperationException(s"$other not supported.")
        }
        childrenExprNodes.foreach(node => childrenExprNodesList.add(node))
        val aggregateFunctionNode = ExpressionBuilder.makeAggregateFunction(
          AggregateFunctionsBuilder.create(registeredFunctions, aggregateFunction),
          childrenExprNodesList,
          sortFields,
          modeToKeyWord(aggExpr.mode),
          ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
        )
        aggregateFunctions.add(aggregateFunctionNode)
      })
    aggregateFunctions
  }

  def genSortFields(
      context: SubstraitContext,
      child: SparkPlan,
      inputAttributes: Seq[Attribute]): util.ArrayList[SortField] = {
    val sortOrder = child match {
      case SortExecTransformer(sortOrder, _, _, _) => sortOrder
      case SortExec(sortOrder, _, _, _) => sortOrder
      case _ => null
    }
    if (sortOrder == null) {
      return null
    }
    val registeredFunctions = context.registeredFunction
    val sortFields = new util.ArrayList[SortField]
    sortOrder.foreach(
      order => {
        val sortField = SortField.newBuilder()
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(order.child, attributeSeq = inputAttributes)
          .doTransform(registeredFunctions)
        sortField.setExpr(exprNode.toProtobuf)
        sortField.setDirectionValue(
          SortExecTransformer.transformSortDirection(order.direction.sql, order.nullOrdering.sql))
        sortFields.add(sortField.build())
      })
    sortFields
  }

  override def isStreaming: Boolean = false

  def numShufflePartitions: Option[Int] = Some(0)

  override protected def withNewChildInternal(newChild: SparkPlan): CHAggregateExecTransformer = {
    copy(child = newChild)
  }
}
