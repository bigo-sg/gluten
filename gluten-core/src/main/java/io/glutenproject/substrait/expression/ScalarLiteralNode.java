package io.glutenproject.substrait.expression;

import io.substrait.proto.Expression;

import com.google.protobuf.ByteString;

import org.apache.spark.sql.types.*;

public class ScalarLiteralNode<T> extends LiteralNode<T> {
  public ScalarLiteralNode(T value, DataType dateType) {
    super(value, dateType);
  }

  @Override
  protected Expression.Literal getLiteral() {
    T value = getValue();
    DataType dataType = getDataType();
    Expression.Literal.Builder literalBuilder = Expression.Literal.newBuilder();

    if (dataType instanceof BooleanType) {
      literalBuilder.setBoolean((Boolean) value);
    } else if (dataType instanceof ByteType) {
      literalBuilder.setI8((Byte) value);
    } else if (dataType instanceof ShortType) {
      literalBuilder.setI16((Short) value);
    } else if (dataType instanceof IntegerType) {
      literalBuilder.setI32((Integer) value);
    } else if (dataType instanceof LongType) {
      literalBuilder.setI64((Long) value);
    } else if (dataType instanceof FloatType) {
      literalBuilder.setFp32((Float) value);
    } else if (dataType instanceof DoubleType) {
      literalBuilder.setFp64((Double) value);
    } else if (dataType instanceof StringType) {
      literalBuilder.setString((String) value);
    } else if (dataType instanceof BinaryType) {
      literalBuilder.setBinary(ByteString.copyFrom((byte[]) value));
    } else if (dataType instanceof DateType) {
      literalBuilder.setDate((Integer) value);
    } else if (dataType instanceof TimestampType) {
      literalBuilder.setTimestamp((Long) value);
    } else {
      throw new UnsupportedOperationException(
          String.format("Type not supported: %s, obj: %s, class: %s",
              dataType.toString(), value.toString(), value.getClass().toString()));
    }

    return literalBuilder.build();
  }

}
