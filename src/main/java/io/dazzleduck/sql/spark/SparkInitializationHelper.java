package io.dazzleduck.sql.spark;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;


public class SparkInitializationHelper {
    public static SparkSession createSparkSession(Config config) {

        var secrets = Secret.readSecrets(config);
        var hadoopProperties = new HashMap<String, String>();
        secrets.forEach((name, map) -> {
            hadoopProperties.putAll(Secret.toHadoopProperties(map));
        });


        var conf = new SparkConf();
        conf.set("spark.sql.ansi.enabled", "true");
        hadoopProperties.forEach((key, value) -> {
            conf.set("spark.hadoop." + key, value);
        });

        return SparkSession.builder().master("local")
                .config(conf)
                .getOrCreate();
    }
}
