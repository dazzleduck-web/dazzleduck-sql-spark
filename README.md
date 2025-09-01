
### Prerequisites
- Spark 3.5.6
- JDK 17
- Docker
### Getting started
- Start Dazzle duck server with `example/data` mounted at `/data` directory
`docker run -ti -v "$PWD/example/data":/data -p 59307:59307 -p 8080:8080 dazzleduck-sql --conf warehouse=/warehouse` 
- Start spark sql with package bin/spark-sql  --packages io.dazzleduck.sql:dazzleduck-sql-spark:0.0.4
- At the spark sql prompt 
`create temp view t ( key string, value string, p int) using io.dazzleduck.sql.spark.ArrowRPCTableProvider  
options ( url = 'jdbc:arrow-flight-sql://localhost:59307?disableCertificateVerification=true&user=admin&password=admin', partition_columns 'p', path '/data/parquet/kv', connection_timeout 'PT60m');`
- query the table with
`select * from t`