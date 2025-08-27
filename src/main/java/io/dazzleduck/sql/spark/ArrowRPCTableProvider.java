package io.dazzleduck.sql.spark;



import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class ArrowRPCTableProvider implements TableProvider, DataSourceRegister {

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        throw new RuntimeException("Cannot infer schema");
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return TableProvider.super.inferPartitioning(options);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        var datasourceOptions = DatasourceOptions.parse(properties);
        return new ArrowRPCTable(schema, getIdentifier(datasourceOptions), datasourceOptions);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public String shortName() {
        return "arrow-kafka";
    }

    public Identifier getIdentifier(DatasourceOptions datasourceOptions) {
        return Identifier.of(new String[0],
                datasourceOptions.identifier() == null? datasourceOptions.path() : datasourceOptions.identifier());
    }
}
