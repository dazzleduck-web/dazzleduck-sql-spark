package io.dazzleduck.sql.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.arrow.flight.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


public class ArrowRPCScan implements Scan, Batch {

    private final StructType requiredPartitionSchema;
    private final InternalRow requiredPartitions;
    private final DatasourceOptions datasourceOptions;
    private final StructType outputSchema;
    private final boolean pushedAggregation;

    private final FlightInfo flightInfo;
    public ArrowRPCScan(StructType outputSchema,
                        boolean pushedAggregation,
                        StructType requiredPartitionSchema,
                        InternalRow requiredPartitions,
                        DatasourceOptions datasourceOptions,
                        FlightInfo flightInfo){
        this.requiredPartitionSchema = requiredPartitionSchema;
        this.requiredPartitions = requiredPartitions;
        this.datasourceOptions = datasourceOptions;
        this.outputSchema = outputSchema;
        this.pushedAggregation = pushedAggregation;
        this.flightInfo  = flightInfo;
    }



    @Override
    public InputPartition[] planInputPartitions() {
        return Arrays.stream(withNewEndpoints(flightInfo)).map(e -> {
                    var buffer = e.serialize();
                    var bytes = new byte[buffer.limit()];
                    buffer.get(bytes);
                    return new ArrowPartition(bytes);
                }).toArray(InputPartition[]::new);
    }

    private static FlightInfo[] withNewEndpoints(FlightInfo flightInfo ) {
        return flightInfo
                .getEndpoints()
                .stream()
                .map( e ->
                        new FlightInfo(flightInfo.getSchema(),
                                flightInfo.getDescriptor(),
                                List.of(e), flightInfo.getBytes(),
                                flightInfo.getRecords()))
                .toArray(FlightInfo[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new ArrowPartitionReaderFactory(outputSchema, requiredPartitionSchema, requiredPartitions, datasourceOptions);
    }

    @Override
    public StructType readSchema() {
        return outputSchema;
    }

    @Override
    public String description() {
        return Scan.super.description();
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return Scan.super.toMicroBatchStream(checkpointLocation);
    }

    @Override
    public ContinuousStream toContinuousStream(String checkpointLocation) {
        return Scan.super.toContinuousStream(checkpointLocation);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        return Scan.super.supportedCustomMetrics();
    }

    @Override
    public CustomTaskMetric[] reportDriverMetrics() {
        return Scan.super.reportDriverMetrics();
    }

    @Override
    public ColumnarSupportMode columnarSupportMode() {
        return Scan.super.columnarSupportMode();
    }

    public boolean hasPushedAggregation() {
        return pushedAggregation;
    }
}
