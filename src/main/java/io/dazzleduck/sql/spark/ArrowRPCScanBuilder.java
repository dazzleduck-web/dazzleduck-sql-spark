package io.dazzleduck.sql.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;
import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class ArrowRPCScanBuilder implements ScanBuilder,
        SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns,
        SupportsPushDownLimit,
        SupportsPushDownAggregates {

    private static final Logger log = LoggerFactory.getLogger(ArrowRPCScanBuilder.class);
    private final StructType sourceSchema;
    private final DuckDBExpressionSQLBuilder dialect;
    private final DatasourceOptions datasourceOptions;
    private StructType outputSchema;
    private final  StructType sourcePartitionSchema;
    private Predicate[] pushedPredicates;
    private int limit = -1;
    private Aggregation pushedAggregation = null;
    private FlightInfo flightInfo;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private boolean completePushedPredicates = false;
    private boolean completeAggregationPushdown = false;

    public ArrowRPCScanBuilder(StructType sourceSchema,
                               DatasourceOptions datasourceOptions) {
        this.dialect = new DuckDBExpressionSQLBuilder(sourceSchema);
        this.sourceSchema = sourceSchema;
        this.outputSchema = sourceSchema;
        this.datasourceOptions = datasourceOptions;
        this.sourcePartitionSchema = getPartitionSchema(sourceSchema, datasourceOptions.partitionColumns());
    }

    @Override
    public Scan build() {
        try {
            FlightInfo flightInfoToSend;
            if(flightInfo == null) {
                var queryObject = QueryBuilderV2.build(sourceSchema, sourcePartitionSchema, datasourceOptions, outputSchema,
                        pushedPredicates, limit, dialect);
                flightInfoToSend = ArrowRPCScan.getFlightInfo(datasourceOptions, queryObject);
            } else {
               flightInfoToSend = flightInfo;
            }
            return new ArrowRPCScan(
                outputSchema,
                pushedAggregation != null,
                new StructType(),
                InternalRow.empty(),
                datasourceOptions,
                flightInfoToSend);
        } catch (Exception e) {
                throw new RuntimeException(e);
        }
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        outputSchema = requiredSchema;
    }

    @Override
    public boolean pushLimit(int limit) {
        this.limit = limit;
        return true;
    }

    @Override
    public boolean isPartiallyPushed() {
        return limit >= 0;
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        var pushedAggregationList = new ArrayList<Predicate>();
        var notPushed = new ArrayList<Predicate>();
        for( var p : predicates) {
            var res = compileExpression(p, dialect);
            if(res.isDefined()) {
                pushedAggregationList.add(p);
            } else {
                notPushed.add(p);
            }
        }
        this.pushedPredicates = pushedAggregationList.toArray(new Predicate[0]);
        this.completePushedPredicates = pushedPredicates.length == predicates.length;
        return notPushed.toArray(new Predicate[0]);
    }

    @Override
    public Predicate[] pushedPredicates() {
        return this.pushedPredicates;
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation) {
        if(pushedPredicates!= null && pushedPredicates.length > 0 && !completePushedPredicates ){
            return false;
        }
        var pushedAggregationSchema = AggregationUtil.getSchemaForPushedAggregation(
                aggregation,
                sourceSchema);
        if(pushedAggregationSchema.isPresent()) {
            var queryObject = QueryBuilderV2.buildForAggregation(sourceSchema, sourcePartitionSchema, datasourceOptions, pushedPredicates, aggregation,  limit, dialect);
            try {
                var flightInfo = ArrowRPCScan.getFlightInfo(datasourceOptions, queryObject);
                this.flightInfo = flightInfo;
                if( flightInfo.getEndpoints().size() < 2 ) {
                    this.completeAggregationPushdown = true;
                    outputSchema = pushedAggregationSchema.get();
                    pushedAggregation = aggregation;
                    return true;
                } else {
                    return false;
                }
            } catch (JsonProcessingException | SQLException e) {
                log.atError().setCause(e).log("Error fetching data from client ");
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        if(pushedAggregation != null) {
            assert pushedAggregation == aggregation;
            return true;
        }
        var pushedAggregationSchema = AggregationUtil.getSchemaForPushedAggregation(
                aggregation,
                sourceSchema);
        if (pushedAggregationSchema.isPresent()) {
            outputSchema = pushedAggregationSchema.get();
            pushedAggregation = aggregation;
            return true;
        } else {
            return false;
        }
    }

    private static Option<String> compileExpression(Expression expression,
                                                    V2ExpressionSQLBuilder expressionSQLBuilder) {
        try {
            String built  = expressionSQLBuilder.build(expression);
            return Option.apply(built);
        } catch (Exception e ){
            return Option.empty();
        }
    }


    private static StructType getPartitionSchema(StructType schema, List<String> partitionColumns) {
        var partitionColumnSet = new HashSet<>(partitionColumns);
        var fs = Arrays.stream(schema.fields()).filter(f -> partitionColumnSet.contains(f.name())).toArray(StructField[]::new);
        return new StructType(fs);
    }
}

