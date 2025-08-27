package io.dazzleduck.sql.spark;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ConnectionPool;

import java.sql.SQLException;

public class DuckDBInitializationHelper {
    public static void initializeDuckDB(Config config) throws SQLException {
        var secret = Secret.readSecrets(config);
        try (var connection = ConnectionPool.getConnection()) {
            Secret.loadSecrets(connection, secret);
        }
    }
}
