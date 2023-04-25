package io.glutenproject.substrait.expression;

import io.substrait.proto.Expression;

import io.glutenproject.substrait.type.*;

abstract class LiteralNodeWithValue<T> extends LiteralNode {
  private final T value;

  public LiteralNodeWithValue(T value, TypeNode typeNode) {
    super(typeNode);
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  @Override
  protected Expression.Literal getLiteral() {
    T value = getValue();
    Expression.Literal.Builder literalBuilder = Expression.Literal.newBuilder();
    updateLiteralBuilder(literalBuilder, value);
    return literalBuilder.build();
  }

  abstract protected void updateLiteralBuilder(Expression.Literal.Builder literalBuilder, T value);
}
