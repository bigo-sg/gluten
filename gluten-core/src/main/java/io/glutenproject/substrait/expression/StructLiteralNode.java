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

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import io.substrait.proto.Expression;
import io.glutenproject.substrait.type.TypeNode;
import io.glutenproject.substrait.type.StructNode;

import java.util.ArrayList;

class StructLiteralNode extends LiteralNode {
  private final GenericInternalRow row;

  public StructLiteralNode(GenericInternalRow row, TypeNode typeNode) {
    super(typeNode);
    this.row= row;
  }

  @Override
  protected Expression.Literal getLiteral() {
    ArrayList<TypeNode> fieldTypes = ((StructNode)getTypeNode()).getFieldTypes();
    Object[] values = row.values();

    Expression.Literal.Struct.Builder structBuilder = Expression.Literal.Struct.newBuilder();
    for (int i=0; i<values.length; ++i) {
      LiteralNode elementNode = ExpressionBuilder.makeLiteral(values[i], fieldTypes.get(i));
      Expression.Literal element = elementNode.getLiteral();
      structBuilder.addFields(element);
    }

    Expression.Literal.Builder literalBuilder = Expression.Literal.newBuilder();
    literalBuilder.setStruct(structBuilder.build());
    return literalBuilder.build();
  }
}

