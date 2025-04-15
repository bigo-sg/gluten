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
package org.apache.gluten.util;

import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.Type;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Convertor to convert Flink LogicalType to velox data Type */
public class LogicalTypeConverter {

    public static Type toVLType(LogicalType logicalType) {
        if (logicalType instanceof RowType) {
            RowType flinkRowType = (RowType) logicalType;
            List<Type> fieldTypes = flinkRowType.getChildren().stream().
                    map(LogicalTypeConverter::toVLType).
                    collect(Collectors.toList());
            return new io.github.zhztheplayer.velox4j.type.RowType(
                    flinkRowType.getFieldNames(),
                    fieldTypes);
        } else if (logicalType instanceof BooleanType) {
            return new io.github.zhztheplayer.velox4j.type.BooleanType();
        } else if (logicalType instanceof TinyIntType) {
            return new io.github.zhztheplayer.velox4j.type.TinyIntType();
        } else if (logicalType instanceof SmallIntType) {
            return new io.github.zhztheplayer.velox4j.type.SmallIntType();
        }else if (logicalType instanceof IntType) {
            return new IntegerType();
        } else if (logicalType instanceof BigIntType) {
            return new io.github.zhztheplayer.velox4j.type.BigIntType();
        } else if (logicalType instanceof FloatType) {
            return new io.github.zhztheplayer.velox4j.type.DoubleType();
        } else if (logicalType instanceof DoubleType) {
            return new io.github.zhztheplayer.velox4j.type.DoubleType();
        } else if (logicalType instanceof VarCharType) {
            return new io.github.zhztheplayer.velox4j.type.VarCharType();
        } else if (logicalType instanceof VarBinaryType) {
            return new io.github.zhztheplayer.velox4j.type.VarbinaryType();
        } else if (logicalType instanceof DateType) {
            return new io.github.zhztheplayer.velox4j.type.DateType();
        } else if (logicalType instanceof TimestampType) {
            // TODO: may need precision
            return new io.github.zhztheplayer.velox4j.type.TimestampType();
        } else if (logicalType instanceof DayTimeIntervalType) {
            return new io.github.zhztheplayer.velox4j.type.IntervalDayTimeType();
        } else if (logicalType instanceof YearMonthIntervalType) {
            return new io.github.zhztheplayer.velox4j.type.IntervalYearMonthType();
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            return new io.github.zhztheplayer.velox4j.type.DecimalType(
                    decimalType.getPrecision(),
                    decimalType.getScale());
        } else {
            throw new RuntimeException("Unsupported logical type: " + logicalType);
        }
    }

    public static LogicalType toFlinkType(Type type) {
        if (type instanceof IntegerType) {
            return new IntType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.BigIntType) {
            return new BigIntType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.VarCharType) {
            return new VarCharType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.DoubleType) {
            return new DoubleType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.BooleanType) {
            return new BooleanType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.TimestampType) {
            return new TimestampType(3);
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.DateType) {
            return new DateType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.DecimalType) {
            io.github.zhztheplayer.velox4j.type.DecimalType decimalType = (io.github.zhztheplayer.velox4j.type.DecimalType) type;
            return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.SmallIntType) {
            return new SmallIntType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.VarbinaryType) {
            return new VarBinaryType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.TinyIntType) {
            return new TinyIntType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.IntervalDayTimeType) {
            return new DayTimeIntervalType(DayTimeResolution.DAY_TO_SECOND);
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.IntervalYearMonthType) {
            return new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH);
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.RowType) {
            io.github.zhztheplayer.velox4j.type.RowType velox4jRowType = (io.github.zhztheplayer.velox4j.type.RowType) type;
            List<RowField> rowFields = new ArrayList<>();
            for (int i = 0; i < velox4jRowType.getChildren().size(); ++i) {
                LogicalType rowFieldType = toFlinkType(velox4jRowType.getChildren().get(i));
                String rowFieldName = velox4jRowType.getNames().get(i);
                RowField rowField = new RowField(rowFieldName, rowFieldType);
                rowFields.add(rowField);
            }
            return new RowType(rowFields);
        } else {
            throw new RuntimeException("Unsuported type:" + type);
        }
    }
}
