package io.dazzleduck.sql.spark;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

public class MinioContainerTestUtil {

    public static final int MINIO_S3_PORT = 9000;
    public static final int MINIO_MGMT_PORT = 9001;
    public static String TEST_BUCKET_NAME = "test-bucket";

    public static Map<String, String> duckDBSecretForS3Access(MinIOContainer minio) {
        var uri = URI.create(minio.getS3URL());
        return Map.of("TYPE", "S3",
                "KEY_ID", minio.getUserName(),
                "SECRET", minio.getPassword(),
                "ENDPOINT", uri.getHost() + ":" + uri.getPort(),
                "USE_SSL", "false",
                "URL_STYLE", "path");
    }


    public static MinIOContainer createContainer(String alias, Network network) {
        return new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withExposedPorts(MINIO_S3_PORT, MINIO_MGMT_PORT);
    }

    public static MinioClient createClient(MinIOContainer minio) {
        return MinioClient.builder().endpoint(minio.getS3URL())
                .credentials(minio.getUserName(), minio.getPassword()).build();
    }

    public static void uploadDirectory(MinioClient minioClient, String bucketName,
                                       String directoryPath, String objectPrefix) throws IOException {
        Path dirPath = Paths.get(directoryPath);
        if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
            throw new IllegalArgumentException("Invalid directory path: " + directoryPath);
        }

        try (Stream<Path> files = Files.walk(dirPath)) {
            files.filter(Files::isRegularFile)
                    .forEach(file -> {
                        String objectName = objectPrefix + dirPath.relativize(file);
                        try {
                            uploadFile(minioClient, bucketName, objectName, file.toString());
                        } catch (IOException e) {
                            System.err.println("Error uploading file: " + file + ", error: " + e.getMessage());
                        }
                    });
        }
    }

    private static void uploadFile(MinioClient minioClient, String bucketName, String objectName, String filePath) throws IOException {
        File file = new File(filePath);
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(fileInputStream, file.length(), -1)
                    .build());
        } catch (Exception e) {
            throw new IOException("Failed to upload " + filePath + " to " + objectName + " in bucket " + bucketName, e);
        }
    }
}
