package io.dazzleduck.sql.spark.expression;

import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;

import java.util.Objects;

public record  LiteralValue<T>(T value, DataType dataType) implements Literal<T> {
    @Override
    public String toString() {
        if (Objects.requireNonNull(dataType) instanceof StringType) {
            return "'%s'".formatted(value);
        } else {
            return value.toString();
        }
    }
}
