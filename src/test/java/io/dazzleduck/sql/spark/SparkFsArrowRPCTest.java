package io.dazzleduck.sql.spark;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;


public class SparkFsArrowRPCTest {
    private static SparkSession spark;
    private static FlightServer flightServer;
    private static final BufferAllocator bufferAllocator = new RootAllocator();
    private static final int port = 33334;
    private static final String url = String.format("jdbc:arrow-flight-sql://localhost:%s?useEncryption=false&disableCertificateVerification=true&user=admin&password=admin", port);
    private static final String host = "0.0.0.0";
    private static Closeable service;
    private static String localPath;
    private static String s3Path;
    private static final String localTable = "table1";
    private static final String s3Table = "s3_table1";

    private static final String identifierTable = "id_table";
    private static final String localPathTable = "local_path_table";
    private static final String s3PathTable = "s3_path_table";
    private static String schemaDDL  = "time timestamp, key string, value string, quantity bigint, size int, price decimal(10,3), s struct<i1 int>, partition int";

    private static final String schemaEvolutionRpcTable = "schema_evolution_rpc";
    private static final String schemaEvolutionLocalTable = "schema_evolution_local";
    private static final String schemaOfEvolutionTable  = "key string, value string, nested struct< a : int, b : int, c : int>, array array<int>, p int";
    private static String schemaEvolutionTablePath;

    public static Network network = Network.newNetwork();
    public static MinIOContainer minio =
            MinioContainerTestUtil.createContainer("minio", network);
    public static MinioClient minioClient;
    public static String partitionColumns = "partition";


    private static void createLocalTable(String schemaDDL,
                                         String tableName , String path) {
        spark.sql(String.format("CREATE TEMPORARY VIEW  %s (%s) USING parquet OPTIONS ( path  '%s')", tableName, schemaDDL, path)).show();
        spark.sql(String.format("select * from %s", tableName)).show();
    }

    @BeforeAll
    public static void setup() throws Exception {
        minio.start();
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinioContainerTestUtil.TEST_BUCKET_NAME).build());
        localPath  = System.getProperty("user.dir") + "/example/data/parquet/spark_fs_test";
        schemaEvolutionTablePath = System.getProperty("user.dir") + "/example/data/parquet/kv";
        MinioContainerTestUtil.uploadDirectory(minioClient,  MinioContainerTestUtil.TEST_BUCKET_NAME, localPath, "spark_fs_test/");
        s3Path = String.format("s3a://%s/%s", MinioContainerTestUtil.TEST_BUCKET_NAME, "spark_fs_test");
        var sc = MinioContainerTestUtil.duckDBSecretForS3Access(minio).entrySet().stream().map( e ->
                "{" + "key = " + e.getKey() +"," + "value = \"" + e.getValue() +  "\"}").collect(Collectors.joining(","));
        var secretsConfigStr = String.format("secrets = { sec1 = %s}", "[" + sc + "]" );
        var secretsConfig = ConfigFactory.parseString(secretsConfigStr);
        var config = ConfigFactory.load();
        var configWithFallback = config.withFallback(secretsConfig);
        spark = SparkInitializationHelper.createSparkSession(configWithFallback);
        DuckDBInitializationHelper.initializeDuckDB(configWithFallback);
        FlightTestUtil.createFsServiceAnsStart(port);

        createLocalTable(schemaDDL, localTable, localPath);
        createLocalTable(schemaDDL, s3Table, s3Path);

        createRPCTestTableWithRemoteId(schemaDDL, identifierTable, "t1");
        createRPCTestTable(schemaDDL, localPathTable, localPath, "partition" );
        createRPCTestTable(schemaDDL, s3PathTable, s3Path, "partition");

        createLocalTable(schemaOfEvolutionTable, schemaEvolutionLocalTable, schemaEvolutionTablePath);
        createRPCTestTable(schemaOfEvolutionTable, schemaEvolutionRpcTable, schemaEvolutionTablePath, "p");
    }

    private static void createRPCTestTable(String schema, String name, String path, String partitionColumns) {
        String createSql = String.format("CREATE TEMP VIEW %s (%s) " +
                "USING " + ArrowRPCTableProvider.class.getName() + " " +
                "OPTIONS ( " +
                "url '%s'," +
                "path '%s'," +
                "username 'admin'," +
                "password 'admin'," +
                "partition_columns '%s'," +
                "connection_timeout 'PT10M'," +
                "parallelize 'true'" +
                ")", name, schema, url, path, partitionColumns);
        spark.sql(createSql);
    }

    private static void createRPCTestTableWithRemoteId(String schema, String name, String remoteIdentifier ) {
        String createSql = String.format("CREATE TEMP VIEW %s (%s) " +
                "USING " + ArrowRPCTableProvider.class.getName() + " " +
                "OPTIONS ( " +
                "url '%s'," +
                "identifier '%s'," +
                "username 'admin'," +
                "password 'admin'," +
                "partition_columns 'partition'," +
                "connection_timeout 'PT10M'" +
                ")", name, schema, url, remoteIdentifier);
        spark.sql(createSql);
    }

    public static String[] getTestSQLs() {
        return new String[]{
                "select s.i1 from %s order by key",
                "select count(*) from %s",
                "select count(*) from %s where partition = 100",
                "select count(s.i1) from %s where partition = 100",
                "select *, s.i1 from %s order by key",
                "select partition from %s where partition = 1",
                "select * from %s where partition = 1",
                "select * from %s where key  = 'k1'",
                "select * from %s where s.i1 = 1",
                "select count(s.i1), s.i1 from %s group by s.i1 order by s.i1",
                "select count(*) from %s group by s.i1 order by s.i1",
                "select count(*), s.i1 from %s group by s.i1 order by s.i1",
                "select count(*), sum(quantity), sum(size), sum(price), min(quantity), max(quantity), partition from %s where partition = 1 group by partition",
                "select count(*), sum(quantity), sum(size), sum(price), avg(price), min(quantity), max(quantity), partition from %s where partition = 1 group by partition",
                "select count(*), to_date(time) from %s group by to_date(time) order by to_date(time)",
                "select count(*), month(time), year(time) from %s group by month(time), year(time), second(time) order by month(time)",
                "select count(*), sum(quantity), sum(size), sum(price), avg(price), min(quantity), max(quantity), partition + 2 from %s where partition = 1 group by partition + 2",
                "select count(*), sum(quantity), sum(size), sum(price), avg(price), min(quantity), max(quantity), date_trunc('day', time) from %s where partition = 1 group by date_trunc( 'day', time)",
        };
    }

    public static String[] getTestSchemaEvolutionSQLs() {
        return new String[]{
                "select p, array from %s order by p",
                "select count(*) from %s where nested.c is null order by count(*)"
        };
    }

    @ParameterizedTest
    @MethodSource("getTestSQLs")
    void testSql(String sql) {
        test(sql,  localPathTable);
    }

    @ParameterizedTest
    @MethodSource("getTestSchemaEvolutionSQLs")
    void testSchemaEvolutionSql(String sql) {
        test(sql, schemaEvolutionLocalTable, schemaEvolutionRpcTable);
    }

    @Test
    void testPathTable() {
        var sql = "select * from %s order by key";
        test(sql, localPathTable);
    }

    @Test
    void testS3Table() {
        var sql = "select * from %s order by key";
        test(sql, s3PathTable);
    }

    private void test(String sql, String expectedTable, String resultTable) {
        String  expectedTableSql = String.format(sql, expectedTable);
        String  replaceTableSql = String.format(sql, resultTable);
        SparkTestHelper.assertEqual(spark, expectedTableSql, replaceTableSql);
    }
    private void test(String sql,
                      String resultTable) {
        test(sql, localTable, resultTable);
    }

    @Test
    public void testRPCScan() {
        var flightclient = FlightClient
                .builder()
                .allocator(new RootAllocator())
                .location(Location.forGrpcInsecure("localhost", port))
                .intercept(AuthUtils.createClientMiddlewareFactory("admin",
                                "admin",
                                Map.of())).build();

        var flightSqlClient = new FlightSqlClient( flightclient);
        flightSqlClient.execute("select 1");
    }

    @Test
    public void testConnectionPool() throws Exception {
        var options = DatasourceOptions.parse(Map.of("url", url, "connection_timeout", "PT10M"));

        var schema = "\"1\"  string";
        var headers = new FlightCallHeaders();
        var schemaOption = new HeaderCallOption(headers);
        var info = FlightSqlClientPool.INSTANCE.getInfo(options, "select 1", schemaOption);
        Assertions.assertNotNull(info);
        try( var stream = FlightSqlClientPool.INSTANCE.getStream(options, info.getEndpoints().get(0))) {

            var root = stream.getRoot();
            var batch = 0;
            while (stream.next()){
                    batch+=1;
                    System.out.println(root.contentToTSVString());
            }
        }
    }

    @AfterAll
    public static void stopAll() throws InterruptedException, IOException {
        spark.close();
        //flightServer.close();
        //service.close();
        //bufferAllocator.close();
    }
}

