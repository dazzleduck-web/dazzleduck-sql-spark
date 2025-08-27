package io.dazzleduck.sql.spark;

import org.apache.arrow.flight.FlightInfo;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

public class ArrowPartitionReaderFactory implements PartitionReaderFactory {

    private final StructType requiredPartitionSchema;
    private final InternalRow requiredPartitions;
    private final StructType outputSchema;
    private final DatasourceOptions datasourceOptions;

    public ArrowPartitionReaderFactory(
            StructType outputSchema,
            StructType requiredPartitionSchema,
            InternalRow requiredPartitions,
            DatasourceOptions datasourceOptions) {
        this.requiredPartitionSchema = requiredPartitionSchema;
        this.requiredPartitions = requiredPartitions;
        this.datasourceOptions = datasourceOptions;
        this.outputSchema = outputSchema;
    }
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        throw new RuntimeException("Row reader in not supported");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition)  {
        ArrowPartition partition1 = (ArrowPartition) partition;
        var buffer = ByteBuffer.wrap(partition1.bytes());
        FlightInfo flightInfo;
        try {
             flightInfo = FlightInfo.deserialize(buffer);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return new ArrowRpcReader(flightInfo, outputSchema, requiredPartitionSchema, requiredPartitions, datasourceOptions);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
}
record ArrowPartition(byte[] bytes) implements InputPartition, Serializable {

}

