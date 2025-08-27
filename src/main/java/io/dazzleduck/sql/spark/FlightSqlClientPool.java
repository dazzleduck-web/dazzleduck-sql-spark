package io.dazzleduck.sql.spark;


import org.apache.arrow.driver.jdbc.ArrowFlightConnection;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum FlightSqlClientPool implements Closeable {
    INSTANCE;
    private static final Duration CLOSE_DELAY = Duration.ofMinutes(2);
    private static final Logger logger = LoggerFactory.getLogger(FlightSqlClientPool.class);
    private final Map<String, ClientAndCreationTime> cache = new ConcurrentHashMap<>();
    private final RootAllocator allocator = new RootAllocator();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    FlightSqlClientPool() {

    }

    // Change the signature and return type
    private static FlightSqlClient getPrivateClientFromHandler(ArrowFlightSqlClientHandler handler) {
        try {
            // The class and field name are the same
            Field clientField = ArrowFlightSqlClientHandler.class.getDeclaredField("sqlClient");
            clientField.setAccessible(true);
            // Cast to the real class
            return (FlightSqlClient) clientField.get(handler);
        } catch (Exception e) { // Catch broader exceptions for simplicity
            throw new RuntimeException(e);
        }
    }

    private static ArrowFlightSqlClientHandler getPrivateClientHandlerFromConnection(ArrowFlightConnection connection) {
        try {
            // The class and field name are the same
            Field clientHandler = ArrowFlightConnection.class.getDeclaredField("clientHandler");
            clientHandler.setAccessible(true);
            // Cast to the real class
            return (ArrowFlightSqlClientHandler) clientHandler.get(connection);
        } catch (Exception e) { // Catch broader exceptions for simplicity
            throw new RuntimeException(e);
        }
    }

    private static CallOption[] getPrivateCallOptions(ArrowFlightSqlClientHandler arrowFlightSqlClientHandler) {
        try {
            Method callOptions = ArrowFlightSqlClientHandler.class.getDeclaredMethod("getOptions");
            callOptions.setAccessible(true);
            return (CallOption[]) callOptions.invoke(arrowFlightSqlClientHandler);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private String getKey(DatasourceOptions options) {
        return options.url() + options.getSourceOptions()
                .entrySet()
                .stream()
                .sorted()
                .map(e -> e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(","));
    }

    private ClientAndCreationTime getClient(DatasourceOptions options) {
        var mapKey = getKey(options);
        var timeout = options.connectionTimeout();
        return cache.compute(mapKey, (key, oldValue) -> {
            if (oldValue == null || oldValue.timestamp < System.currentTimeMillis() + timeout.toMillis()) {
                if (oldValue != null) {
                    scheduledExecutorService.schedule(() -> closeClient(oldValue.flightClient), CLOSE_DELAY.toMillis(), TimeUnit.MILLISECONDS);
                }
                try {
                    var connection = new ArrowFlightJdbcDriver().connect(options.url(), options.properties());
                    var ch = getPrivateClientHandlerFromConnection(connection);
                    var sqlClient = getPrivateClientFromHandler(ch);
                    var coptions = getPrivateCallOptions(ch);
                    return new ClientAndCreationTime(System.currentTimeMillis(), sqlClient, coptions);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                return oldValue;
            }
        });
    }

    public FlightInfo getInfo(DatasourceOptions options, String query, CallOption... callOptions) {
        var client = getClient(options);
        return client.flightClient.execute(query,
                Stream.concat(Arrays.stream(client.callOptions),
                        Arrays.stream(callOptions)).toArray(CallOption[]::new));
    }

    public FlightStream getStream(DatasourceOptions options, FlightEndpoint endpoint, CallOption... callOptions) throws SQLException {
        var client = getClient(options);
        return client.flightClient.getStream(endpoint.getTicket(), Stream.concat(Arrays.stream(client.callOptions), Arrays.stream(callOptions)).toArray(CallOption[]::new));
    }

    private void closeClient(FlightSqlClient flightClient) {
        try {
            flightClient.close();
        } catch (Exception e) {
            logger.atError().setCause(e).log("ERROR closing client" + flightClient);
        }
    }

    @Override
    public synchronized void close() {
        var it = cache.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            ClientAndCreationTime v = entry.getValue();
            try {
                v.flightClient().close();
            } catch (Exception e) {
                // log  the exception
                logger.atError().setCause(e).log("Error closing the connection : " + v);
            }
            it.remove();
        }
        allocator.close();
        scheduledExecutorService.shutdown();
    }

    record ClientAndCreationTime(long timestamp, FlightSqlClient flightClient, CallOption[] callOptions) {
    }
}
