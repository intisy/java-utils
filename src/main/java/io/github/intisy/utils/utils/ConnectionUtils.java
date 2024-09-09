package io.github.intisy.utils.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;

public class ConnectionUtils {
    public static String curl2(HttpURLConnection connection, String token, String payload) {
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
    public static void printOutput(HttpURLConnection connection) throws IOException {
        System.out.println(getOutput(connection));
    }
}
