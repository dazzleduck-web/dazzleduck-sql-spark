package io.dazzleduck.sql.spark;

import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects$;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.stream.Collectors;

public class DuckDBExpressionSQLBuilder extends V2ExpressionSQLBuilder {
    private final StructType schema;
    private final JdbcDialect jdbcDialect = JdbcDialects$.MODULE$.get("jdbc:postgresql");

    public DuckDBExpressionSQLBuilder(StructType schema){
        this.schema = schema;
    }

    @Override
    public String visitLiteral(Literal literal) {
        return jdbcDialect.compileValue(CatalystTypeConverters.convertToScala(literal.value(), literal.dataType()))
                .toString();
    }

    @Override
    public String visitNamedReference(NamedReference namedReference){
        DataType type = getType(namedReference);
        String id = Arrays.stream(namedReference.fieldNames()).map(jdbcDialect::quoteIdentifier)
                .collect(Collectors.joining("."));
        return id;
        //return visitCast(id, type);
    }

    public String visitCast(String l , DataType dataType) {
        return buildCast(l, dataType);
    }

    protected String visitSQLFunction(String funcName, String[] inputs) {
        if (jdbcDialect.isSupportedFunction(funcName)) {
            return super.visitSQLFunction(funcName, inputs);
        } else {
            throw new UnsupportedOperationException(funcName);
        }
    }

    public String visitAggregateFunction(
            String funcName, boolean isDistinct, String[] inputs) {
        if (jdbcDialect.isSupportedFunction(funcName)) {
            return super.visitSQLFunction(funcName, inputs);
        } else {
            throw new UnsupportedOperationException(funcName);
        }
    }

    private DataType getType(NamedReference namedReference) {
        DataType result = null;
        DataType input = schema;
        for(String element : namedReference.fieldNames()) {
            result = getType((StructType) input, element);
            input = result;
        }
        return result;
    }

    private static DataType getType(StructType current, String element) {
        return current.apply(element).dataType();
    }

    public String buildCast(String expression, DataType dataType) {
        String c = buildCast(dataType);
        return String.format("CAST(%s as %s)", expression, c);
    }

    private String buildCast(DataType dataType) {
        switch (dataType) {
            case StructType s -> {
                var fields = s.fields();
                String inner = Arrays.stream(fields).map(f -> {
                    String cast = buildCast(f.dataType());
                    return jdbcDialect.quoteIdentifier(f.name()) + " " + cast;
                }).collect(Collectors.joining(", "));
                return String.format("STRUCT(%s)", inner);
            }
            case MapType m -> {
                String keyType = buildCast(m.keyType());
                String valueType = buildCast(m.valueType());
                return String.format("MAP(%s, %s", keyType, valueType);
            }
            case DecimalType d -> {
                return String.format("Decimal(%s, %s)", d.precision(), d.scale());
            }
            case ArrayType a -> {
                String childCast = buildCast(a.elementType());
                return String.format("%s[]", childCast);
            }
            case TimestampType timestampType -> {
                return "TIMESTAMP";
            }
            case StringType stringType -> {
                return "VARCHAR";
            }
            case IntegerType integerType -> {
                return "INTEGER";
            }
            case DateType dateType -> {
                return "DATE";
            }
            case LongType longType -> {
                return "BIGINT";
            }
            case DoubleType doubleType -> {
                return "DOUBLE";
            }
            case FloatType floatType -> {
                return "FLOAT";
            }
            case null, default -> throw new IllegalArgumentException(dataType.toString());
        }
    }


    public static String translateSchema(StructType st) {
        return Arrays.stream(st.fields()).map( structField -> {
            var fname = structField.name();
            return fname + " " + translateDataType(structField.dataType());
        }).collect(Collectors.joining(","));
    }

    public static String buildCast(StructType dataType, String source, DuckDBExpressionSQLBuilder dialect) {
        var prefix = Arrays.stream(dataType.fields()).map( f -> "NULL::%s".formatted(translateDataType(f.dataType()))).collect(Collectors.joining(","));
        var suffix = Arrays.stream(dataType.fieldNames()).map(f -> dialect.build(new FieldReference(new String[]{f}))).collect(Collectors.joining(", "));
        return "FROM (VALUES(%s)) t(%s)\n".formatted(prefix, suffix) +
                "WHERE false\n" +
                "UNION ALL BY NAME\n" +
                "FROM %s".formatted(source);
    }
    public static String translateDataType(DataType dataType) {
        switch (dataType) {
            case IntegerType i -> {
                return "int";
            }
            case LongType l -> {
                return "bigint";
            }
            case DoubleType d -> {
                return "double";
            }
            case FloatType f -> {
                return "float";
            }
            case BooleanType b -> {
                return "boolean";
            }
            case StringType s -> {
                return "varchar";
            }
            case DecimalType d -> {
                return "decimal(" + d.precision() + "," + d.scale() + ")";
            }
            case BinaryType b -> {
                return "binary";
            }
            case StructType st -> {
                var inner = Arrays.stream(st.fields()).map( structField -> {
                    var fname = structField.name();
                    return fname + " " + translateDataType(structField.dataType());
                }).collect(Collectors.joining(","));
                return String.format("STRUCT(%s)", inner);
            }
            case ArrayType arrayType  -> {
                return translateDataType(arrayType.elementType()) + "[]";
            }
            case TimestampType t -> {
                return "timestamp";
            }
            case DateType t -> {
                return "date";
            }

            case MapType m -> {
                String keyType = translateDataType(m.keyType());
                String valueType = translateDataType(m.valueType());
                return String.format("MAP(%s, %s", keyType, valueType);
            }

            case TimestampNTZType t -> {
                return "timestampz";
            }
            default -> {
                throw new UnsupportedOperationException("Not supported : " + dataType );
            }
        }
    }
}
