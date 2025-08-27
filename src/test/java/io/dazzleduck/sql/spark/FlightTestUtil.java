package io.dazzleduck.sql.spark;


import com.amazonaws.services.dynamodbv2.xspec.M;
import com.typesafe.config.ConfigFactory;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import io.dazzleduck.sql.flight.server.Main;

import java.io.Closeable;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;


public class FlightTestUtil {
    public static void createFsServiceAnsStart(int port) throws IOException, NoSuchAlgorithmException, InterruptedException {
        String[] args = {"--conf", "port=" + port, "--conf", "useEncryption=false", "--conf", "accessMode=RESTRICTED"};
        Main.main(args);
        System.out.println("Running service ");
        Thread.sleep(2000);
    }
}
