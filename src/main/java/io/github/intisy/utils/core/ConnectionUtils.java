package io.github.intisy.utils.core;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * This class provides utility methods for handling HTTP connections.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
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
        connection.getRequestProperties().forEach((key, value) -> curl.append(" -H \"").append(key).append(": ").append(value.get(0)).append("\""));
        curl.append(" -d '").append(payload).append("' ").append(connection.getURL());
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

    /**
     * Sends a JSON payload through the output stream of the specified HTTP connection.
     *
     * @param jsonPayload The JSON object to be sent as the request payload.
     * @param connection  The HTTP connection to which the payload will be written.
     * @throws IOException If an I/O error occurs while writing to the output stream.
     */
    public static void connectionPayload(JsonObject jsonPayload, HttpURLConnection connection) throws IOException {
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = jsonPayload.toString().getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
    }

    /**
     * Sends an authorized POST request to the specified API URL with the provided authorization key and processes the response.
     *
     * @param apiUrl The URL of the API to which the request will be sent.
     * @param key    The authorization key to be included in the request header.
     * @return A JsonObject representing the server's response. Contains an additional "code" property indicating the HTTP response code.
     */
    public static JsonObject authorizedRequest(String apiUrl, String key) {
        try {
            HttpURLConnection connection = post(apiUrl);
            connection.setRequestProperty("Authorization", key);
            return handleResponse(connection);
        } catch (IOException e) {
            return handleException(e);
        }
    }

    /**
     * Handles an exception and returns a JSON object containing error details.
     *
     * @param e The exception to be handled. Provides details about the error that occurred.
     * @return A JsonObject containing two properties:
     *         - "message": A descriptive message about the error.
     *         - "type": A fixed string "error" indicating the nature of the response.
     */
    public static JsonObject handleException(Exception e) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("message", "An error occurred while handling the request: " + e.getMessage());
        jsonObject.addProperty("type", "error");
        return jsonObject;
    }

    /**
     * Sends an authorized HTTP request to the specified API URL with the given authorization key
     * and JSON payload, and processes the server's response.
     *
     * @param apiUrl      The URL of the API to which the request will be sent.
     * @param key         The authorization key to be included in the request header.
     * @param jsonPayload The JSON object to be sent as the request payload.
     * @return A JsonObject representing the server's response. Contains an additional "code" property
     *         indicating the HTTP response code.
     */
    public static JsonObject authorizedRequest(String apiUrl, String key, JsonObject jsonPayload) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", key);
        return headerRequest(apiUrl, headers, jsonPayload);
    }

    /**
     * Sends a POST request to the specified API URL with given headers and JSON payload,
     * and returns the server's response as a JSON object.
     * If an error occurs during the request, it handles the exception and returns
     * an error JSON object.
     *
     * @param apiUrl      The URL of the API to which the request will be sent.
     * @param headers     A map containing key-value pairs representing the request headers.
     * @param jsonPayload The JSON object to be sent as the request payload.
     * @return A JsonObject representing the server's response. If an exception occurs,
     *         returns a JSON object containing error details.
     */
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

    /**
     * Sends a JSON payload as a POST request to the specified API URL, processes the
     * server's response, and handles potential exceptions.
     *
     * @param apiUrl      The URL of the API to which the request will be sent.
     * @param jsonPayload The JSON object to be sent as the request payload.
     * @return A JsonObject representing the server's response. Contains an additional
     *         "code" property for the HTTP response code. If an exception occurs,
     *         returns a JsonObject with error details.
     */
    public static JsonObject request(String apiUrl, JsonObject jsonPayload) {
        try {
            HttpURLConnection connection = post(apiUrl);
            connectionPayload(jsonPayload, connection);
            return handleResponse(connection);
        } catch (IOException e) {
            return handleException(e);
        }
    }

    /**
     * Sends a POST request to the specified API URL, processes the server's response,
     * and handles any potential I/O exceptions that may occur during the request.
     *
     * @param apiUrl The URL of the API to which the POST request will be sent.
     * @return A JsonObject representing the server's response. If the request is
     *         successful, the response includes a "code" property indicating the HTTP
     *         response code and the parsed JSON payload. If an exception occurs, returns
     *         a JsonObject with error details, including a "message" and a "type".
     */
    public static JsonObject request(String apiUrl) {
        try {
            HttpURLConnection connection = post(apiUrl);
            return handleResponse(connection);
        } catch (IOException e) {
            return handleException(e);
        }
    }

    /**
     * Sends a POST request to the specified API URL and returns the configured
     * HttpURLConnection object. This method sets the request method to POST,
     * the content type to "application/json", and enables output for the connection.
     *
     * @param apiUrl The URL of the API to which the POST request will be sent.
     * @return An HttpURLConnection object configured for the POST request.
     * @throws IOException If an I/O error occurs during the connection setup.
     */
    public static HttpURLConnection post(String apiUrl) throws IOException {
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        return connection;
    }

    /**
     * Processes the response from an HTTP connection, parses the response content
     * into a JSON object, and includes the HTTP response code as a property within
     * the returned JSON object. If the response code is not 200 (HTTP OK), logs the
     * error details.
     *
     * @param connection The {@code HttpURLConnection} from which to read the response.
     *                   Must not be null and should already be configured and connected.
     * @return A {@code JsonObject} representing the response. Includes a "code" property
     *         with the HTTP response code. If the response is in error, the response body
     *         is also included in the returned JSON object.
     * @throws IOException If an I/O error occurs while reading the response or
     *                     if the response cannot be parsed as a valid JSON object.
     */
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
        reader.setStrictness(Strictness.LENIENT);
        JsonObject jsonObject;
        try {
            jsonObject = JsonParser.parseReader(reader).getAsJsonObject();
        } catch (Exception e) {
            throw new IOException("Failed to parse JSON: " + e.getMessage());
        }
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("Response code: " + responseCode + " response: " + jsonObject.toString());
        }
        jsonObject.addProperty("code", responseCode);
        br.close();
        return jsonObject;
    }

    /**
     * Reads all lines from the provided BufferedReader and returns them as a single string.
     *
     * @param br The BufferedReader instance from which to read the lines. Must not be null.
     * @return A string containing the concatenated lines read from the BufferedReader.
     * @throws IOException If an I/O error occurs while reading from the BufferedReader.
     */
    public static String read(BufferedReader br) throws IOException {
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            response.append(line);
        }
        return response.toString();
    }
}
