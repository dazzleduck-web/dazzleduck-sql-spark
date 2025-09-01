package io.dazzleduck.sql.spark;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Secret {

    public static final Pattern regex = Pattern.compile("ENC\\s*?\\((.*?)\\)");

    public static final String CREATE_SECRET_SQL =  "CREATE SECRET %s ( %s )";

    public static final Pattern accountPattern = Pattern.compile(
            "^s3://([^/]+)/?$");

    public static final Set<String> SUPPORTED =
            Set.of("KEY_ID", "SECRET", "REGION", "SCOPE", "ENDPOINT", "URL_STYLE",
                    "USE_SSL",
                    "URL_COMPATIBILITY_MOD");

    public static final Map<String, String> HADOOP_CONF_KEY_MAPPING = Map.of(
            "KEY_ID", "access.key",
            "SECRET", "secret.key",
            "REGION", "region",
            "ENDPOINT", "endpoint",
            "USE_SSL", "connection.ssl.enabled",
            "URL_STYLE", "path.style.access"
    );

    public static final Map<String, Function<String, String>> HADOOP_CONF_VALUE_MAPPING = Map.of(
            "KEY_ID", v -> v,
            "SECRET", v -> v,
            "REGION", v -> v,
            "ENDPOINT", v -> v,
            "USE_SSL", v -> v,
            "URL_STYLE", v -> v.toLowerCase(Locale.ROOT).equals("path") ? "true" : "false"
    );

    public static Map<String, Map<String, String>> readSecrets(Config config) {
        final var encryptionProvider = PLAIN_TEXT;
        if (config.hasPath("secrets")) {
            var secrets = config.getConfig("secrets");
            var result = new HashMap<String, Map<String, String>>();
            secrets.root().keySet().forEach(name -> {
                var list = secrets.getConfigList(name);
                var map = new HashMap<String, String>();
                list.forEach(c -> {
                    map.put(c.getString("key"),
                            decrypt(c.getString("value"), encryptionProvider));

                });
                result.put(name, map);
            });
            return result;
        } else {
            return Map.of();
        }
    }

    public static void loadSecrets(Connection connection, Map<String, Map<String, String>> secretMap) throws SQLException {
        for (var kv : secretMap.entrySet()) {
            loadSecret(connection, kv.getKey(), kv.getValue());
        }
        ;
    }

    public static void loadSecret(Connection connection,
                                  String name,
                                  Map<String, String> params) throws SQLException {
        var paramString = params.entrySet().stream().map(e -> e.getKey() + " " + "'" + e.getValue() + "'").collect(Collectors.joining(", "));
        String sql = String.format(CREATE_SECRET_SQL, name, paramString);
        ConnectionPool.execute(connection, sql);
    }

    public static String decrypt(String configValue, EncryptionProvider encryptionProvider) {
        Matcher matcher = regex.matcher(configValue);
        if (matcher.find()) {
            // Get text inside parentheses
            var value = matcher.group(1);
            return encryptionProvider.decrypt(value);
        } else {
            return configValue;
        }
    }

    public static Map<String, String> toHadoopProperties(Map<String, String> duckDBConfig) {
        var scope = duckDBConfig.get("SCOPE");
        String scopePrefix = "";
        if (scope != null) {
            var matcher = accountPattern.matcher(scope);
            matcher.find();
            var value = matcher.group(1);
            scopePrefix = "bucket." + value + ".";
        }
        String prefix = "fs.s3a." + scopePrefix;
        Map<String, String> result = new HashMap<>();
        duckDBConfig.forEach((key, value) -> {
            String hadoopProp = HADOOP_CONF_KEY_MAPPING.get(key);
            if (hadoopProp != null) {
                result.put(prefix + hadoopProp,
                        HADOOP_CONF_VALUE_MAPPING.get(key).apply(value));
            }
        });
        return result;
    }

    public static interface EncryptionProvider {
        public String encrypt(String input);

        public String decrypt(String input);
    }

    public static EncryptionProvider PLAIN_TEXT = new EncryptionProvider() {
        @Override
        public String encrypt(String input) {
            return input;
        }

        @Override
        public String decrypt(String input) {
            return input;
        }
    };
}
