package io.github.intisy.utils.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;

/**
 * This class provides utility methods for handling HTTP connections.
 */
public class ConnectionUtils {

    /**
     * Generates a curl command based on the provided HTTP connection, token, and payload.
     *
     * @param connection The HTTP connection to be used.
     * @param token      The authorization token to be included in the curl command. If null, no token will be included.
     * @param payload    The payload to be sent in the curl command.
     * @return A curl command as a string.
     */
    public static String curl(HttpURLConnection connection, String token, String payload) {
        StringBuilder curl = new StringBuilder();
        if (token != null)
            curl.append("curl -X ").append(connection.getRequestMethod()).append(" -H \"Authorization: Bearer ").append(token).append("\"");
        connection.getRequestProperties().forEach((key, value) -> {
            curl.append(" -H \"").append(key).append(": ").append(value.get(0)).append("\"");
        });
        curl.append(" -d '").append(payload.toString()).append("' ").append(connection.getURL());
        System.out.println(connection.getRequestProperties());
        return curl.toString();
    }

    /**
     * Retrieves the output from the error stream of the provided HTTP connection.
     *
     * @param connection The HTTP connection to be used.
     * @return The output from the error stream as a string.
     * @throws IOException If an I/O error occurs while reading from the error stream.
     */
    public static String getOutput(HttpURLConnection connection) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            response.append(line);
        }
        br.close();
        return response.toString();
    }

    /**
     * Prints the output from the error stream of the provided HTTP connection.
     *
     * @param connection The HTTP connection to be used.
     * @throws IOException If an I/O error occurs while reading from the error stream.
     */
    public static void printOutput(HttpURLConnection connection) throws IOException {
        System.out.println(getOutput(connection));
    }
}
