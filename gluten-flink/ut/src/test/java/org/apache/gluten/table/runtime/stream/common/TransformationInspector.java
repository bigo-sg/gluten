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
package org.apache.gluten.table.runtime.stream.common;

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.table.runtime.operators.GlutenTwoInputOperator;

import io.github.zhztheplayer.velox4j.serde.Serde;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility for inspecting a Flink transformation tree to verify that operators have been offloaded
 * to Gluten's native engine. Provides methods to recursively collect all transformations and check
 * whether the operator at a given position is a specific Gluten class (e.g. {@link
 * GlutenStreamSource}, {@link GlutenOneInputOperator}, {@link GlutenTwoInputOperator}).
 */
public final class TransformationInspector {

  private TransformationInspector() {}

  /**
   * Recursively collects all transformations in the tree rooted at the given list, including nested
   * inputs. The result is ordered from source (leaf) to sink (root), which matches the natural
   * data-flow direction of the pipeline.
   */
  public static List<Transformation<?>> collectAll(List<Transformation<?>> roots) {
    List<Transformation<?>> result = new ArrayList<>();
    Set<Transformation<?>> visited = new HashSet<>();
    collectAll(roots, result, visited);
    Collections.reverse(result);
    return result;
  }

  private static void collectAll(
      List<Transformation<?>> roots,
      List<Transformation<?>> result,
      Set<Transformation<?>> visited) {
    for (Transformation<?> t : roots) {
      if (!visited.add(t)) {
        continue;
      }
      result.add(t);
      collectAll(t.getInputs(), result, visited);
    }
  }

  /**
   * Returns the underlying operator object of a transformation, unwrapping {@link
   * SimpleOperatorFactory} and {@link GlutenOneInputOperatorFactory} if necessary.
   *
   * <p>For {@link LegacySourceTransformation} the operator is returned directly via {@code
   * getOperator()}. For {@link OneInputTransformation} and {@link TwoInputTransformation} the
   * factory is unwrapped. For all other transformation types {@code null} is returned.
   */
  public static Object getOperator(Transformation<?> transformation) {
    if (transformation instanceof LegacySourceTransformation) {
      return ((LegacySourceTransformation<?>) transformation).getOperator();
    }
    StreamOperatorFactory<?> factory = getOperatorFactory(transformation);
    return unwrapOperator(factory);
  }

  /**
   * Returns the JSON string of the native plan node for a Gluten operator, or null if the
   * transformation's operator is not a {@link GlutenOperator}.
   */
  @Nullable
  public static String getPlanJson(Transformation<?> transformation) {
    Object operator = getOperator(transformation);
    if (operator instanceof GlutenOperator) {
      return Serde.toJson(((GlutenOperator) operator).getPlanNode());
    }
    return null;
  }

  /**
   * Checks whether the operator of the given transformation is an instance of the specified class.
   */
  public static boolean isOperatorType(Transformation<?> transformation, Class<?> operatorClass) {
    Object operator = getOperator(transformation);
    return operator != null && operatorClass.isInstance(operator);
  }

  /**
   * Checks whether any transformation in the list has an operator that is an instance of the
   * specified class.
   */
  public static boolean hasOperatorType(
      List<Transformation<?>> transformations, Class<?> operatorClass) {
    for (Transformation<?> t : transformations) {
      Object operator = getOperator(t);
      if (operator != null && operatorClass.isInstance(operator)) {
        return true;
      }
    }
    return false;
  }

  /** Returns the operator factory of a one-input or two-input transformation, or null. */
  private static StreamOperatorFactory<?> getOperatorFactory(Transformation<?> transformation) {
    if (transformation instanceof OneInputTransformation) {
      return ((OneInputTransformation<?, ?>) transformation).getOperatorFactory();
    }
    if (transformation instanceof TwoInputTransformation) {
      return ((TwoInputTransformation<?, ?, ?>) transformation).getOperatorFactory();
    }
    return null;
  }

  /** Unwraps the underlying operator from a factory, handling Gluten and Simple factories. */
  private static Object unwrapOperator(StreamOperatorFactory<?> factory) {
    if (factory == null) {
      return null;
    }
    if (factory instanceof GlutenOneInputOperatorFactory) {
      return ((GlutenOneInputOperatorFactory<?, ?>) factory).getOperator();
    }
    if (factory instanceof SimpleOperatorFactory) {
      return ((SimpleOperatorFactory<?>) factory).getOperator();
    }
    // For other factory types, check if the factory itself implements StreamOperator
    // (some factories are both factory and operator).
    if (factory instanceof StreamOperator) {
      return factory;
    }
    return null;
  }
}
