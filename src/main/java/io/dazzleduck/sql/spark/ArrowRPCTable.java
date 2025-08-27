package io.dazzleduck.sql.spark;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.EnumSet;
import java.util.Set;

import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ;

public class ArrowRPCTable implements Table, SupportsRead {

    public final StructType schema;
    public final Identifier identifier;
    public final DatasourceOptions datasourceOptions;
    public ArrowRPCTable(StructType schema, Identifier identifier, DatasourceOptions datasourceOptions) {
        this.schema = schema;
        this.identifier = identifier;
        this.datasourceOptions = datasourceOptions;
    }
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new ArrowRPCScanBuilder(schema, datasourceOptions);
    }

    @Override
    public String name() {
        return identifier.toString();
    }

    @Override
    public StructType schema() {
        return schema;
    }
    @Override
    public Set<TableCapability> capabilities() {
        return EnumSet.of(BATCH_READ);
    }
}
