package io.glutenproject.substrait.expression;

import io.substrait.proto.Expression;

import com.google.protobuf.ByteString;

import io.glutenproject.substrait.type.*;

public class ScalarLiteralNode<T> extends LiteralNode {
  private final T value;

  public ScalarLiteralNode(T value, TypeNode typeNode) {
    super(typeNode);
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  @Override
  protected Expression.Literal getLiteral() {
    T value = getValue();
    TypeNode typeNode = getTypeNode();
    Expression.Literal.Builder literalBuilder = Expression.Literal.newBuilder();

    if (typeNode instanceof BooleanTypeNode) {
      literalBuilder.setBoolean((Boolean) value);
    } else if (typeNode instanceof I8TypeNode) {
      literalBuilder.setI8((Byte) value);
    } else if (typeNode instanceof I16TypeNode) {
      literalBuilder.setI16((Short) value);
    } else if (typeNode instanceof I32TypeNode) {
      literalBuilder.setI32((Integer) value);
    } else if (typeNode instanceof I64TypeNode) {
      literalBuilder.setI64((Long) value);
    } else if (typeNode instanceof FP32TypeNode) {
      literalBuilder.setFp32((Float) value);
    } else if (typeNode instanceof FP64TypeNode) {
      literalBuilder.setFp64((Double) value);
    } else if (typeNode instanceof StringTypeNode) {
      literalBuilder.setString((String) value);
    } else if (typeNode instanceof BinaryTypeNode) {
      literalBuilder.setBinary(ByteString.copyFrom((byte[]) value));
    } else if (typeNode instanceof DateTypeNode) {
      literalBuilder.setDate((Integer) value);
    } else if (typeNode instanceof TimestampTypeNode) {
      literalBuilder.setTimestamp((Long) value);
    } else {
      throw new UnsupportedOperationException(
          String.format("Type not supported: %s, obj: %s, class: %s",
              typeNode.toString(), value.toString(), value.getClass().toString()));
    }

    return literalBuilder.build();
  }

}
