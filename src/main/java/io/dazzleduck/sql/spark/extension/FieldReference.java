package io.dazzleduck.sql.spark.extension;

import org.apache.spark.sql.connector.expressions.NamedReference;

import java.util.Arrays;

public record FieldReference(String[] parts) implements NamedReference {
    @Override
    public String[] fieldNames() {
        return Arrays.copyOf(parts, parts.length);
    }
}
