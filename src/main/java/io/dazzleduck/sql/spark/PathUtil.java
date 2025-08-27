package io.dazzleduck.sql.spark;

public class PathUtil {

    public static final String S3A_REGEX = "^s3a";
    public static final String S3_REGEX = "^s3";
    public static final String LOCAL_REGEX = "^file:";

    public static String toS3APath(String path) {
        return path.replaceAll(S3_REGEX, "s3a");
    }

    public static String toS3Path(String path) {
        return path.replaceAll(S3A_REGEX, "s3");
    }

    public static String toLocalPath(String path) {
        return path.replaceAll(LOCAL_REGEX, "");
    }

    public static String toDazzleDuckPath(String path) {
        return toLocalPath(toS3Path(path));
    }
}
