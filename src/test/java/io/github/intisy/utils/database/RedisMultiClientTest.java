package io.github.intisy.utils.database;

import io.github.intisy.simple.logger.SimpleLogger; // Assuming you have a logger implementation

import java.io.IOException;

public class RedisMultiClientTest {

    public static void main(String[] args) {
        SimpleLogger logger = new SimpleLogger(); // Use your actual logger
        Redis serverInstance = null;
        Redis client1 = null;
        Redis client2 = null;
        int actualPort;

        try {
            // 1. Start an embedded Redis server instance
            // We use one instance to manage the embedded server lifecycle.
            // Enable allowPortSearch to find a free port if the default (6379) is busy.
            logger.info("Attempting to start embedded Redis server...");
            serverInstance = new Redis("localhost", 6379, true, true, false); // host, port, useEmbedded, allowPortSearch, allowMockFallback
            serverInstance.setLogger(logger);
            serverInstance.connect(); // This will start the embedded server

            if (!serverInstance.isConnected() || !serverInstance.ping()) {
                throw new RuntimeException("Failed to start or connect to the embedded Redis server.");
            }
            actualPort = serverInstance.getPort(); // Get the actual port used (might differ if port search was needed)
            logger.success("Embedded Redis server started successfully on port: " + actualPort);

            // 2. Create two client instances connecting to the embedded server
            // Note: useEmbedded is false for these clients as they connect externally.
            logger.info("Creating Client 1...");
            client1 = new Redis(serverInstance.getHost(), actualPort, false, false, false);
            client1.setLogger(logger);
            client1.connect();
            if (!client1.isConnected() || !client1.ping()) {
                throw new RuntimeException("Client 1 failed to connect.");
            }
            logger.success("Client 1 connected.");


            logger.info("Creating Client 2...");
            client2 = new Redis(serverInstance.getHost(), actualPort, false, false, false);
            client2.setLogger(logger);
            client2.connect();
            if (!client2.isConnected() || !client2.ping()) {
                throw new RuntimeException("Client 2 failed to connect.");
            }
            logger.success("Client 2 connected.");

            // 3. Test Interaction: Client 1 sets data, Client 2 gets data
            String testKey = "multiClientTestKey";
            String testValue = "Hello from Client 1!";

            logger.info("Client 1 setting data: Key='" + testKey + "', Value='" + testValue + "'");
            client1.setData(testKey, testValue);

            logger.info("Client 2 getting data for Key='" + testKey + "'");
            String retrievedValue = client2.getData(testKey);

            if (testValue.equals(retrievedValue)) {
                logger.success("Success! Client 2 retrieved the value set by Client 1: '" + retrievedValue + "'");
            } else {
                logger.error("Failure! Client 2 retrieved unexpected value: '" + retrievedValue + "'");
            }

            // 4. (Optional) Check client list
            // Note: Parsing this string can be brittle. This just prints it.
            logger.info("Checking client list (via Client 1):");
            String clientList = client1.getClientList();
            logger.info(clientList);
            // You could add logic here to check if the list contains >1 client entry.

        } catch (IOException e) {
            logger.error("IOException during Redis operations", e);
        } catch (RuntimeException e) {
            logger.error("Runtime exception during test", e);
        } finally {
            // 5. Clean up: Disconnect clients and stop the server
            logger.info("Cleaning up resources...");
            if (client1 != null && client1.isConnected()) {
                client1.disconnect();
                logger.info("Client 1 disconnected.");
            }
            if (client2 != null && client2.isConnected()) {
                client2.disconnect();
                logger.info("Client 2 disconnected.");
            }
            if (serverInstance != null && serverInstance.isConnected()) {
                // Disconnecting the instance that manages the embedded server will stop it.
                serverInstance.disconnect();
                logger.info("Embedded Redis server instance stopped.");
            }
        }
    }
}