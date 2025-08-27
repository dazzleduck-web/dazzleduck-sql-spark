package io.dazzleduck.sql.spark;

import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.spark.expression.LiteralValue;
import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class QueryBuilderV2Test {

    @Test
    public void testWithoutAggregation() throws SQLException, IOException {
        var datasourceSchema = (StructType) DataType.fromDDL( "key string, value string, p string");
        var partitionSchema = (StructType) DataType.fromDDL("p string");
        var datasourceOptions =new DatasourceOptions("localhost", null, "example/data/parquet/kv", List.of("p"), Duration.ofMinutes(10), new Properties());
        var outputSchema = (StructType) DataType.fromDDL("key string");
        var predicateChildren = new Expression[] {
                new LiteralValue<>(UTF8String.fromString("v2"), DataType.fromDDL("string")),
                new FieldReference(new String[]{"value"})
        };
        var predicates = new Predicate[]{
                new Predicate("=", predicateChildren)
        };
        var sql = QueryBuilderV2.build(datasourceSchema, partitionSchema, datasourceOptions, outputSchema,  predicates, 10, new DuckDBExpressionSQLBuilder(datasourceSchema));
        TestUtils.isEqual("select unnest(['k1'])", sql);
    }
}
