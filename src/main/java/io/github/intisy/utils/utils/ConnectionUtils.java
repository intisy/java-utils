package io.github.intisy.utils.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import io.github.intisy.simple.logger.StaticLogger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

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

    public static void connectionPayload(JsonObject jsonPayload, HttpURLConnection connection) throws IOException {
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = jsonPayload.toString().getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
    }

    public static JsonObject authorizedRequest(String apiUrl, String key) {
        try {
            HttpURLConnection connection = post(apiUrl);
            connection.setRequestProperty("Authorization", key);
            return handleResponse(connection);
        } catch (IOException e) {
            return handleException(e);
        }
    }

    public static JsonObject handleException(Exception e) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("message", "An error occurred while handling the request: " + e.getMessage());
        jsonObject.addProperty("type", "error");
        return jsonObject;
    }

    public static JsonObject authorizedRequest(String apiUrl, String key, JsonObject jsonPayload) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", key);
        return headerRequest(apiUrl, headers, jsonPayload);
    }
    public static JsonObject headerRequest(String apiUrl, Map<String, String> headers, JsonObject jsonPayload) {
        try {
            HttpURLConnection connection = post(apiUrl);
            headers.forEach(connection::setRequestProperty);
            connectionPayload(jsonPayload, connection);
            return handleResponse(connection);
        } catch (IOException e) {
            return handleException(e);
        }
    }

    public static JsonObject request(String apiUrl, JsonObject jsonPayload) {
        try {
            HttpURLConnection connection = post(apiUrl);
            connectionPayload(jsonPayload, connection);
            return handleResponse(connection);
        } catch (IOException e) {
            return handleException(e);
        }
    }

    public static JsonObject request(String apiUrl) throws IOException {
        try {
            HttpURLConnection connection = post(apiUrl);
            return handleResponse(connection);
        } catch (IOException e) {
            return handleException(e);
        }
    }

    public static HttpURLConnection post(String apiUrl) throws IOException {
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        return connection;
    }

    public static JsonObject handleResponse(HttpURLConnection connection) throws IOException {
        int responseCode = connection.getResponseCode();
        BufferedReader br;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        } else {
            br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
        }
        String response = read(br);
        JsonReader reader = new JsonReader(new StringReader(response));
        reader.setLenient(true);
        try {
            JsonObject jsonObject = JsonParser.parseReader(reader).getAsJsonObject();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                StaticLogger.warning(jsonObject.toString());
                if (jsonObject.get("message").getAsString().equals("Invalid key parameter")) {
                    StaticLogger.error("Invalid API key found.");
                    //TODO rework login
                }
            }
            br.close();
            return jsonObject;
        } catch (Exception e) {
            System.err.println("Failed to parse JSON: " + e.getMessage());
            return new JsonObject();
        }
    }

    public static String read(BufferedReader br) throws IOException {
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            response.append(line);
        }
        return response.toString();
    }
}
