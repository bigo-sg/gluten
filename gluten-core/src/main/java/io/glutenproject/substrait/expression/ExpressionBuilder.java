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

package io.glutenproject.substrait.expression;

import io.glutenproject.expression.ConverterUtils;
import io.glutenproject.substrait.type.*;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Contains helper functions for constructing substrait relations. */
public class ExpressionBuilder {
  private ExpressionBuilder() {}

  public static Long newScalarFunction(Map<String, Long> functionMap, String functionName) {
    if (!functionMap.containsKey(functionName)) {
      Long functionId = (long) functionMap.size();
      functionMap.put(functionName, functionId);
      return functionId;
    } else {
      return functionMap.get(functionName);
    }
  }

  public static NullLiteralNode makeNullLiteral(TypeNode typeNode) {
    return new NullLiteralNode(typeNode);
  }

  public static BooleanLiteralNode makeBooleanLiteral(Boolean booleanConstant) {
    return new BooleanLiteralNode(booleanConstant);
  }

  public static IntLiteralNode makeIntLiteral(Integer intConstant) {
    return new IntLiteralNode(intConstant);
  }

  public static IntListNode makeIntList(ArrayList<Integer> intConstants) {
    return new IntListNode(intConstants);
  }

  public static ByteLiteralNode makeByteLiteral(Byte byteConstant) {
    return new ByteLiteralNode(byteConstant);
  }

  public static ShortLiteralNode makeShortLiteral(Short shortConstant) {
    return new ShortLiteralNode(shortConstant);
  }

  public static LongLiteralNode makeLongLiteral(Long longConstant) {
    return new LongLiteralNode(longConstant);
  }

  public static LongListNode makeLongList(ArrayList<Long> longConstants) {
    return new LongListNode(longConstants);
  }

  public static DoubleLiteralNode makeDoubleLiteral(Double doubleConstant) {
    return new DoubleLiteralNode(doubleConstant);
  }

  public static DoubleListNode makeDoubleList(ArrayList<Double> doubleConstants) {
    return new DoubleListNode(doubleConstants);
  }

  public static FloatLiteralNode makeFloatLiteral(Float floatConstant) {
    return new FloatLiteralNode(floatConstant);
  }

  public static DateLiteralNode makeDateLiteral(Integer dateConstant) {
    return new DateLiteralNode(dateConstant);
  }

  public static DateListNode makeDateList(ArrayList<Integer> dateConstants) {
    return new DateListNode(dateConstants);
  }

  public static TimestampLiteralNode makeTimestampLiteral(Long tsConstants) {
    return new TimestampLiteralNode(tsConstants);
  }

  public static StringLiteralNode makeStringLiteral(String strConstant) {
    return new StringLiteralNode(strConstant);
  }

  public static StringListNode makeStringList(ArrayList<String> strConstants) {
    return new StringListNode(strConstants);
  }

  public static BinaryStructNode makeBinaryStruct(byte[][] binary, StructType type) {
    return new BinaryStructNode(binary, type);
  }

  public static BinaryLiteralNode makeBinaryLiteral(byte[] bytesConstant) {
    return new BinaryLiteralNode(bytesConstant);
  }

  public static DecimalLiteralNode makeDecimalLiteral(Decimal decimalConstant) {
    return new DecimalLiteralNode(decimalConstant);
  }

  public static ListLiteralNode makeListLiteral(GenericArrayData array, TypeNode typeNode) {
    return new ListLiteralNode(array, typeNode);
  }

  public static MapLiteralNode makeMapLiteral(ArrayBasedMapData map, TypeNode typeNode) {
    return new MapLiteralNode(map, typeNode);
  }

  public static StructLiteralNode makeStructLiteral(GenericInternalRow row, TypeNode typeNode) {
    return new StructLiteralNode(row, typeNode);
  }

  public static LiteralNode makeLiteral(Object obj, TypeNode typeNode) {
    if (obj == null) {
      return makeNullLiteral(typeNode);
    }

    if (typeNode instanceof BooleanTypeNode) {
      return makeBooleanLiteral((Boolean) obj);
    } else if (typeNode instanceof I8TypeNode) {
      return makeByteLiteral((Byte) obj);
    } else if (typeNode instanceof I16TypeNode) {
      return makeShortLiteral((Short) obj);
    } else if (typeNode instanceof I32TypeNode) {
      return makeIntLiteral((Integer) obj);
    } else if (typeNode instanceof I64TypeNode) {
      return makeLongLiteral((Long) obj);
    } else if (typeNode instanceof FP32TypeNode) {
      return makeFloatLiteral((Float) obj);
    } else if (typeNode instanceof FP64TypeNode) {
      return makeDoubleLiteral((Double) obj);
    } else if (typeNode instanceof DateTypeNode) {
      return makeDateLiteral((Integer) obj);
    } else if (typeNode instanceof TimestampTypeNode) {
      return makeTimestampLiteral((Long) obj);
    } else if (typeNode instanceof StringTypeNode) {
      return makeStringLiteral(obj.toString());
    } else if (typeNode instanceof BinaryTypeNode) {
      return makeBinaryLiteral((byte[]) obj);
    } else if (typeNode instanceof DecimalTypeNode) {
      Decimal decimal = (Decimal) obj;
      checkDecimalScale(decimal.scale());
      return makeDecimalLiteral(decimal);
    } else if (typeNode instanceof ListNode) {
      return makeListLiteral((GenericArrayData) obj, typeNode);
    } else if (typeNode instanceof MapNode) {
      return makeMapLiteral((ArrayBasedMapData) obj, typeNode);
    } else if (typeNode instanceof StructNode) {
      return makeStructLiteral((GenericInternalRow) obj, typeNode);
    } else {
      throw new UnsupportedOperationException(
          String.format("Type not supported: %s, obj: %s, class: %s",
              typeNode.toString(), obj.toString(), obj.getClass().toString()));
    }
  }

  public static LiteralNode makeLiteral(Object obj, DataType dataType, Boolean nullable) {
    TypeNode typeNode = ConverterUtils.getTypeNode(dataType, nullable);
    return makeLiteral(obj, typeNode);
  }

  public static void checkDecimalScale(int scale) {
    if (scale < 0) {
      // Substrait don't support decimal type with negative scale.
      throw new UnsupportedOperationException(String.format(
        "DecimalType with negative scale not supported: %s.", scale));
    }
  }

  public static ScalarFunctionNode makeScalarFunction(
      Long functionId, ArrayList<ExpressionNode> expressionNodes, TypeNode typeNode) {
    return new ScalarFunctionNode(functionId, expressionNodes, typeNode);
  }

  public static SelectionNode makeSelection(Integer fieldIdx) {
    return new SelectionNode(fieldIdx);
  }

  public static SelectionNode makeSelection(Integer fieldIdx, Integer childFieldIdx) {
    return new SelectionNode(fieldIdx, childFieldIdx);
  }

  public static AggregateFunctionNode makeAggregateFunction(
      Long functionId,
      ArrayList<ExpressionNode> expressionNodes,
      String phase,
      TypeNode outputTypeNode) {
    return new AggregateFunctionNode(functionId, expressionNodes, phase, outputTypeNode);
  }

  public static CastNode makeCast(TypeNode typeNode, ExpressionNode expressionNode,
                                  boolean ansiEnabled) {
    return new CastNode(typeNode, expressionNode, ansiEnabled);
  }

  public static StringMapNode makeStringMap(Map<String, String> values) {
    return new StringMapNode(values);
  }

  public static SingularOrListNode makeSingularOrListNode(ExpressionNode value,
                                                          List<ExpressionNode> expressionNodes) {
    return new SingularOrListNode(value, expressionNodes);
  }

  public static WindowFunctionNode makeWindowFunction(
      Integer functionId,
      ArrayList<ExpressionNode> expressionNodes,
      String columnName,
      TypeNode outputTypeNode,
      String upperBound,
      String lowerBound,
      String windowType) {
    return new WindowFunctionNode(functionId, expressionNodes, columnName,
        outputTypeNode, upperBound, lowerBound, windowType);
  }
}
