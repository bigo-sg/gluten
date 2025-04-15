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
package org.apache.gluten.vectorized;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.gluten.util.LogicalTypeConverter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converter between velox RowVector and Flink RowData. */
public class FlinkRowToVLVectorConvertor {

    public static RowVector fromRowData(
            RowData row,
            BufferAllocator allocator,
            Session session,
            RowType rowType) {
        List<FieldVector> arrowVectors = new ArrayList<>(rowType.size());
        for (int i = 0; i < rowType.size(); i++) {
            Type fieldType = rowType.getChildren().get(i);
            String fieldName = rowType.getNames().get(i);
            LogicalType flinkType = LogicalTypeConverter.toFlinkType(fieldType);
            FieldGetter fieldGetter = RowData.createFieldGetter(flinkType, i);
            Object value = fieldGetter.getFieldOrNull(row);
            FieldVector fieldVector = convertFromFlinkTypedValue(value, fieldName, fieldType, allocator);
            arrowVectors.add(fieldVector);
        }
        return session.arrowOps().fromArrowTable(allocator, new Table(arrowVectors));
    }

    static class VLTypeInfo {
        Type type;
        ArrowType fieldType;
        Class<? extends FieldVector> vectorClazz;

        VLTypeInfo(Type type, ArrowType fieldType, Class<? extends FieldVector> vectorClazz) {
            this.type = type;
            this.fieldType = fieldType;
            this.vectorClazz = vectorClazz;
        }
    }

    private static VLTypeInfo getVLTypeInfo(Type type) {
        if (type instanceof IntegerType) {
            return new VLTypeInfo(type, MinorType.INT.getType(), IntVector.class);
        } else if (type instanceof BigIntType) {
            return new VLTypeInfo(type, MinorType.BIGINT.getType(), BigIntVector.class);
        } else if (type instanceof VarCharType) {
            return new VLTypeInfo(type, MinorType.VARCHAR.getType(), VarCharVector.class);
        } else if (type instanceof BooleanType) {
            return new VLTypeInfo(type, MinorType.BIT.getType(), BitVector.class);
        } else if (type instanceof DoubleType) {
            return new VLTypeInfo(type, MinorType.FLOAT8.getType(), Float8Vector.class);
        } else if (type instanceof TimestampType) {
            return new VLTypeInfo(type, MinorType.TIMESTAMPMILLI.getType(), TimeStampMilliVector.class);
        } else if (type instanceof RowType) {
            return new VLTypeInfo(type, MinorType.STRUCT.getType(), StructVector.class);
        } else {
            throw new RuntimeException("Unsupported field type:" + type);
        }
    }

    private static void setFieldValue(FieldVector vector, Object value) {
        if (value == null) {
            vector.setNull(0);
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(0, (int) value);
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(0, (long) value);
        } else if (vector instanceof VarCharVector) {
            if (value instanceof String) {
                ((VarCharVector) vector).setSafe(0, ((String) value).getBytes());
            } else if (value instanceof StringData) {
                ((VarCharVector) vector).setSafe(0, ((StringData) value).toBytes());
            } else {
                throw new RuntimeException("Flink string type value only support String/StringData.");
            }
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(0, (double) value);
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(0, ((Boolean) value) ? 1 : 0);
        } else if (vector instanceof TimeStampMilliVector) {
            ((TimeStampMilliVector) vector).setSafe(0, ((TimestampData) value).getMillisecond());
        } else {
            throw new RuntimeException("Unsupported to set value to vector:" + vector.getClass().getName());
        }
        vector.setValueCount(1);
    }

    private static FieldVector convertFromFlinkTypedValue(Object value, String name, Type type, BufferAllocator allocator) {
        if (type instanceof IntegerType) {
            IntVector intVector = new IntVector(name, allocator);
            setFieldValue(intVector, value);
            return intVector;
        } else if (type instanceof BigIntType) {
            BigIntVector bigIntVector = new BigIntVector(name, allocator);
            setFieldValue(bigIntVector, value);
            return bigIntVector;
        } else if (type instanceof VarCharType) {
            VarCharVector varCharVector = new VarCharVector(name, allocator);
            setFieldValue(varCharVector, value);
            return varCharVector;
        } else if (type instanceof DoubleType) {
            Float8Vector float8Vector = new Float8Vector(name, allocator);
            setFieldValue(float8Vector, value);
            return float8Vector;
        } else if (type instanceof BooleanType) {
            BitVector bitVector = new BitVector(name, allocator);
            setFieldValue(bitVector, value);
            return bitVector;
        } else if (type instanceof TimestampType) {
            TimeStampMilliVector timeStampVector = new TimeStampMilliVector(name, allocator);
            setFieldValue(timeStampVector, value);
            return timeStampVector;
        } else if (type instanceof RowType) {
            StructVector structVector = StructVector.empty(name, allocator);
            RowType rowType = (RowType) type;
            RowData rowData = (RowData) value;
            for (int i = 0; i < rowType.getChildren().size(); ++i) {
                Type rowFieldType = rowType.getChildren().get(i);
                String rowFieldName = rowType.getNames().get(i);
                VLTypeInfo typeInfo = getVLTypeInfo(rowFieldType);
                FieldVector rowFieldVector = structVector.addOrGet(rowFieldName, FieldType.nullable(typeInfo.fieldType), typeInfo.vectorClazz);
                if (rowData == null) {
                    structVector.setNull(0);
                } else {
                    LogicalType flinkLogicalType = LogicalTypeConverter.toFlinkType(rowFieldType);
                    FieldGetter rowFieldGetter = RowData.createFieldGetter(flinkLogicalType, i);
                    Object rowFieldValue = rowFieldGetter.getFieldOrNull(rowData);
                    setFieldValue(rowFieldVector, rowFieldValue);
                }
            }
            structVector.setValueCount(1);
            return structVector;
        } else {
            throw new RuntimeException("Unsupported type:" + type);
        }
    }

    private static Object convertToFlinkTypedValue(FieldVector value) {
        assert(value.getValueCount() == 1);
        if (value.isNull(0)) {
            return null;
        }
        if (value instanceof IntVector) {
            return ((IntVector) value).get(0);
        } else if (value instanceof BigIntVector) {
            return ((BigIntVector) value).get(0);
        } else if (value instanceof VarCharVector) {
            return ((VarCharVector) value).get(0);
        } else if (value instanceof Float8Vector) {
            return ((Float8Vector) value).get(0);
        } else if (value instanceof Float4Vector) {
            return ((Float4Vector) value).get(0);
        } else if (value instanceof BitVector) {
            return ((BitVector) value).get(0);
        } else if (value instanceof TimeStampVector) {
            return ((TimeStampVector) value).get(0);
        } else if (value instanceof StructVector) {
            StructVector structVector = (StructVector) value;
            if (structVector.getChildrenFromFields().size() == 0) {
                return new GenericRowData(0);
            } else {
                GenericRowData rowData = new GenericRowData(structVector.getChildrenFromFields().size());
                for (int i = 0; i < structVector.getChildrenFromFields().size(); ++i) {
                    FieldVector f = structVector.getChildrenFromFields().get(i);
                    Object convertedValue = convertToFlinkTypedValue(f);
                    rowData.setField(i, convertedValue);
                }
                return rowData;
            }
        } else {
            String errMsg = String.format("Unsupported field type:%s, with value:%s", value.getClass().getName(), value);
            throw new RuntimeException(errMsg);
        }
    }

    public static List<RowData> toRowData(
            RowVector rowVector,
            BufferAllocator allocator,
            Session session,
            RowType rowType) {
        List<RowData> rowDatas = new ArrayList<>(rowVector.getSize());
        FieldVector fieldVector = Arrow.toArrowVector(allocator, rowVector.loadedVector());
        Map<String, Integer> fieldNameAndIndex = new HashMap<String, Integer>();
        for (int i = 0; i < fieldVector.getChildrenFromFields().size(); ++i) {
            fieldNameAndIndex.put(fieldVector.getChildrenFromFields().get(i).getName(), i);
        }
        for (int i = 0; i < rowVector.getSize(); i++) {
            List<Object> fieldValues = new ArrayList<>(rowType.size());
            for (int j = 0; j < rowType.size(); j++) {
                String rowFieldName = rowType.getNames().get(j);
                if (fieldNameAndIndex.containsKey(rowFieldName)) {
                    int fieldIndex = fieldNameAndIndex.get(rowFieldName);
                    FieldVector fieldValue = fieldVector.getChildrenFromFields().get(fieldIndex);
                    Object convertedValue = convertToFlinkTypedValue(fieldValue);
                    fieldValues.add(convertedValue);
                } else {
                    fieldValues.add(null);
                }
            }
            rowDatas.add(GenericRowData.of(fieldValues.toArray()));
        }
        return rowDatas;
    }
}
