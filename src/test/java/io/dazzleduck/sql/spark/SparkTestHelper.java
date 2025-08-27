package io.dazzleduck.sql.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class SparkTestHelper {

    public static void assertEqual(SparkSession sparkSession, String expectedSql, String resultSql) {
        assertEqual(sparkSession, expectedSql, resultSql, false);
    }
    public static void assertEqual(SparkSession sparkSession, String expectedSql, String resultSql, boolean detailedError) {
        var expected = sparkSession.sql(expectedSql);
        var result = sparkSession.sql(resultSql);
        Row[] e = (Row[] )expected.collect();
        Row[] r = (Row[] )result.collect();
        assertArrayEquals(e, r, detailedError ? String.format("\n %s\n %s\n %s\n %s",
                expectedSql, resultSql,
                expected.showString(20, 100, false),
                result.showString(20, 100, false)) : "Result do not match");
    }
}
