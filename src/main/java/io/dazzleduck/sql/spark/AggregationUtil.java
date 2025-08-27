package io.dazzleduck.sql.spark;


import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.spark.sql.connector.expressions.*;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils;
import org.apache.spark.sql.types.*;

import java.util.Optional;

public class AggregationUtil {
    public static Optional<StructType> getSchemaForPushedAggregation(Aggregation aggregation, StructType schema) {
        var res = new StructType();
        Expression[] expressions = aggregation.groupByExpressions();
        if(expressions != null) {
            for (var expression : expressions) {
                if(expression == null) {
                    return Optional.empty();
                }
                if(expression instanceof  NamedReference namedReference) {
                    res = res.add(getField(schema, namedReference));
                } else if (expression instanceof Cast c && c.expression() instanceof  FieldReference f && f.fieldNames().length == 1) {
                    var name = String.join("_", f.fieldNames());
                    res = res.add(new StructField(String.format("cast(%s)", name), c.dataType(), true, Metadata.empty()));
                } else if (expression instanceof Extract e && (e.source() instanceof Cast || e.source() instanceof  FieldReference)) {
                    var name = String.join(e.source().toString());
                    res = res.add(new StructField(String.format("extract(%s)", name), LongType$.MODULE$, true, Metadata.empty()));
                } else {
                    return Optional.empty();
                }
            }
        }
        for(var agg : aggregation.aggregateExpressions()) {
            if(agg instanceof Max ignored1) {
                var p = processMinOrMax(agg, schema);
                if(p.isPresent()){
                    res = res.add(p.get());
                } else {
                    return Optional.empty();
                }
            } else if ( agg instanceof Min ignored1) {
                var p = processMinOrMax(agg, schema);
                if (p.isPresent()) {
                    res = res.add(p.get());
                } else {
                    return Optional.empty();
                }
            } else if (agg instanceof Count count)  {
                if (V2ColumnUtils.extractV2Column(count.column()).isDefined() && !count.isDistinct()) {
                    var columnName = V2ColumnUtils.extractV2Column(count.column()).get();
                    var f = new StructField(String.format("cast(count($%s) as long)", columnName), DataTypes.LongType, true,
                            Metadata.empty() );
                    res = res.add(f);
                } else {
                    return Optional.empty();
                }
            } else if (agg instanceof CountStar ignored1) {
                var f = new StructField("cast(count(*) as long)", DataTypes.LongType, true,
                        Metadata.empty());
                res  = res.add(f);
            } else  if (agg instanceof Sum sum) {
                if (V2ColumnUtils.extractV2Column(sum.column()).isDefined()) {
                    var columnName = V2ColumnUtils.extractV2Column(sum.column()).get();
                    var sumColName = String.format("sum(%s)", columnName);
                    StructField f;
                    var col = schema.apply(columnName);
                    if(col.dataType() instanceof  LongType || col.dataType() instanceof IntegerType) {
                        f = new StructField(sumColName, new DecimalType(), false, Metadata.empty());
                    } else if(col.dataType() instanceof  ShortType) {
                        f = new StructField(sumColName, DataTypes.LongType, false, Metadata.empty());
                    } else if (col.dataType() instanceof FloatType || col.dataType() instanceof DoubleType) {
                        f = new StructField(sumColName, DataTypes.DoubleType, false, Metadata.empty());
                    } else {
                        f = new StructField(sumColName, col.dataType(), false, Metadata.empty());
                    }
                    res = res.add(f);
                } else {
                    return Optional.empty();
                }
            } else if( agg instanceof Avg avg) {
                if (V2ColumnUtils.extractV2Column(avg.column()).isDefined()) {
                    var columnName = V2ColumnUtils.extractV2Column(avg.column()).get();
                    var sumColName = String.format("avg(%s)", columnName);
                    StructField f = new StructField(sumColName, DataTypes.DoubleType, false, Metadata.empty());
                    res = res.add(f);
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
        }

        if(res.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(res);
        }
    }

    private static Optional<StructField> processMinOrMax( AggregateFunc agg,
                                                          StructType schema) {
        String[] columnNames;
        String aggType;
        if(agg instanceof Min min) {
            var col = extractV2Column(min.column());
            if(col != null) {
                aggType = "min";
                columnNames = col;
            } else {
                return Optional.empty();
            }
        } else if( agg instanceof Max max) {
            var col = extractV2Column(max.column());
            if (col!=null) {
                aggType = "max";
                columnNames = col;
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
        StructField f = null;
        DataType t = schema;
        for(String c : columnNames) {
            f = ((StructType)t).apply(c);
            t = f.dataType();
        }

        assert f != null;
        var name = aggType + "(" + f.name() +")";

        var dt = f.dataType();
        if(dt instanceof BooleanType ||
                dt instanceof  LongType ||
                dt instanceof ShortType ||
                dt instanceof IntegerType ||
                dt instanceof FloatType ||
                dt instanceof DoubleType ||
                dt instanceof DateType ||
                dt instanceof StringType ||
                dt instanceof  TimestampType ||
                dt instanceof  TimestampNTZType) {
            var field = f.copy(name, f.dataType(), f.nullable(), f.metadata());
            return Optional.of(field);
        }
        return Optional.empty();
    }

    public static StructField getField(StructType schema, NamedReference reference) {
        DataType dataType = schema;
        StructField res  = null;
        for(var curr : reference.fieldNames()) {
            var currentStruct = (StructType) dataType;
            res = currentStruct.apply(curr);
            dataType = res.dataType();
        }
        return res;
    }

    private static String[] extractV2Column(Expression expression) {
        if(expression instanceof NamedReference nr) {
            return nr.fieldNames();
        } else {
            return null;
        }
    }
}
