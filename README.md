bin/spark-sql  --packages io.dazzleduck.sql:dazzleduck-sql-spark:0.0.4
docker run -ti -v "$PWD/example/data":/data -p 59307:59307 -p 8080:8080 dazzleduck-sql --conf warehouse=/warehouse
create temp view t ( key string, value string, p int) using io.dazzleduck.sql.spark.ArrowRPCTableProvider  
options ( url = 'jdbc:arrow-flight-sql://localhost:59307?disableCertificateVerification=true&user=admin&password=admin', partition_columns 'p', path '/data/parquet/kv', connection_timeout 'PT60m');