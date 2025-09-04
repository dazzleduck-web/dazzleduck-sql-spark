package io.dazzleduck.sql.spark;


import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.flight.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;


public class ArrowRpcReader implements PartitionReader<ColumnarBatch> {

    protected final FlightInfo flightInfo;
    private final StructType requiredPartitionSchema;
    private final InternalRow requiredPartitions;
    private final StructType outputSchema;
    private final DatasourceOptions datasourceOptions;
    private FlightStream flightStream;
    private boolean init;

    private static final Logger logger = LoggerFactory.getLogger(ArrowRpcReader.class);

    /**
     *
     * @param flightInfo
     * @param requiredPartitionSchema
     * @param requiredPartitions
     * @param datasourceOptions
     */
    public ArrowRpcReader(FlightInfo flightInfo,
                          StructType outputSchema,
                          StructType requiredPartitionSchema,
                          InternalRow requiredPartitions,
                          DatasourceOptions datasourceOptions){
        this.flightInfo = flightInfo;
        this.requiredPartitionSchema = requiredPartitionSchema;
        this.requiredPartitions = requiredPartitions;
        this.datasourceOptions = datasourceOptions;
        this.outputSchema = outputSchema;
    }

    private  void init()  {
        if (!init) {
            try {
                flightStream = FlightSqlClientPool.INSTANCE.getStream(datasourceOptions, flightInfo.getEndpoints().get(0));
                init = true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean next() throws IOException {
        if(!init) {
            init();
        }
        try {
            return flightStream.next();
        } catch (Exception e){
            try {
                flightStream.close();
            } catch (Exception ex) {
                logger.atError().setCause(ex).log("Error closing the stream");
                throw new RuntimeException(ex);
            }
            logger.atError().setCause(e).log("Error closing the stream");
            throw new RuntimeException(e);
        }
    }

    @Override
    public ColumnarBatch get() {
        VectorSchemaRoot vectorSchemaRoot = flightStream.getRoot();
        ColumnVector[] partitionVectors = createPartitionVector(vectorSchemaRoot.getRowCount());
        List<ArrowColumnVector> rpcVectors = vectorSchemaRoot
                .getFieldVectors()
                .stream().map(ArrowColumnVector::new).toList();
        ColumnVector[] result =
                Stream.concat(rpcVectors.stream(), Arrays.stream(partitionVectors)).toArray(ColumnVector[]::new);
        return new ColumnarBatch(result, vectorSchemaRoot.getRowCount());
    }

    @Override
    public void close() throws IOException {
        try {
            flightStream.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ColumnVector[] createPartitionVector(int size) {
        StructField[] fields = requiredPartitionSchema.fields();
        ColumnVector[] result = new ColumnVector[fields.length];
        for(int index = 0 ; index < fields.length; index ++){
            StructField field = fields[index];
            ConstantColumnVector vector = new ConstantColumnVector(size, field.dataType());
            var dataType = field.dataType();
            if( dataType instanceof IntegerType ||  dataType instanceof  DateType ) {
                vector.setInt(requiredPartitions.getInt(index));
            } else if ( dataType instanceof LongType || dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
                vector.setLong(requiredPartitions.getLong(index));
            } else  if( dataType instanceof  StringType) {
                vector.setUtf8String(requiredPartitions.getUTF8String(index));
            } else if (dataType instanceof DecimalType ) {
                DecimalType d = (DecimalType) dataType;
                vector.setDecimal(
                        requiredPartitions.getDecimal(index, d.precision(), d.scale()), d.precision());
            } else {
                throw new IllegalStateException("Unexpected value: " + field.dataType().getClass());
            }
            result[index] = vector;
        }
        return result;
    }
}
