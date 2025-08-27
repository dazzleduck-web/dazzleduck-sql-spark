package io.dazzleduck.sql.spark;

import java.io.Serializable;
import java.util.*;
import java.time.Duration;

public record DatasourceOptions(
        String url,
        String identifier,
        String path,
        List<String> partitionColumns,
        Duration connectionTimeout,
        Properties properties) implements Serializable {
    public static final String CONNECTION_TIMEOUT = "connection_timeout";
    public static final String IDENTIFIER_KEY = "identifier";
    public static final String PATH_KEY = "path";
    public static final String URL_KEY = "url";
    public static final String PARTITION_COLUMNS_KEY = "partition_columns";
    public static Set<String> EXCLUDE_PROPS = Set.of(IDENTIFIER_KEY, PATH_KEY, URL_KEY, PARTITION_COLUMNS_KEY, CONNECTION_TIMEOUT);
    public static DatasourceOptions parse(Map<String, String> properties){
        var url = properties.get(URL_KEY);
        var identifier = properties.get(IDENTIFIER_KEY);
        var path = properties.get(PATH_KEY);
        var partitionColumnString = properties.get(PARTITION_COLUMNS_KEY);
        var timeoutString =properties.get(CONNECTION_TIMEOUT);
        if(timeoutString == null) {
            throw new RuntimeException("timeout value is required");
        }
        var timeout = Duration.parse(timeoutString);
        var propsWithout = new Properties();
        properties.forEach( (key, value) -> {
            if(!EXCLUDE_PROPS.contains(key))
                propsWithout.put(key, value);
        });
        List<String> partitionColumns = partitionColumnString ==null? List.of(): Arrays.stream(partitionColumnString.split(",")).toList();
        return new DatasourceOptions(url, identifier, path, partitionColumns, timeout, propsWithout);
    }

    public Map<String, String> getSourceOptions() {
        if(path != null) {
            return Map.of(PATH_KEY, PathUtil.toDazzleDuckPath(path));
        }
        if(identifier != null){
            return Map.of(IDENTIFIER_KEY, identifier);
        }
        return Map.of();
    }
}
