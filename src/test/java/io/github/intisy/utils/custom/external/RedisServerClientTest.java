package io.github.intisy.utils.custom.external;

import io.github.intisy.simple.logger.SimpleLogger; // Assuming you have a logger implementation
import io.github.intisy.utils.custom.external.Redis;

import java.io.IOException;

public class RedisServerClientTest {

    public static void main(String[] args) {
        SimpleLogger logger = new SimpleLogger(); // Use your actual logger
        Redis serverClient = null; // This instance will manage the embedded server AND act as a client
        Redis client2 = null;      // This instance will be a pure client

        try {
            // 1. Create the first Redis instance to act as the server + client
            // useEmbedded=true tells it to start the embedded server on connect()
            // allowPortSearch=true allows it to find a free port if needed
            logger.info("Creating server+client instance and starting embedded Redis...");
            serverClient = new Redis("localhost", 6379, true, true, false); // host, port, useEmbedded=true, allowPortSearch=true, allowMockFallback=false
            serverClient.setLogger(logger);
            serverClient.connect(); // Starts the embedded server and connects this instance to it

            if (!serverClient.isConnected() || !serverClient.ping()) {
                 throw new RuntimeException("Failed to start or connect the embedded Redis server instance.");
            }
            logger.success("Embedded Redis server started by serverClient on host: " + serverClient.getHost() + ", port: " + serverClient.getPort());

            // 2. Create the second client instance connecting to the first one's server
            // useEmbedded=false as it's just connecting externally
            // Use the actual host and port from the serverClient instance
            logger.info("Creating Client 2 to connect to serverClient's embedded server...");
            client2 = new Redis(serverClient.getHost(), serverClient.getPort(), false, false, false); // useEmbedded=false
            client2.setLogger(logger);
            client2.connect(); // Connects to the existing embedded server

            if (!client2.isConnected() || !client2.ping()) {
                 throw new RuntimeException("Client 2 failed to connect to the embedded server.");
            }
            logger.success("Client 2 connected successfully.");

            // 3. Test Interaction - Bidirectional
            String key1 = "serverClientKey";
            String value1 = "Data from server+client";
            String key2 = "client2Key";
            String value2 = "Data from pure client";

            // serverClient sets data, client2 retrieves
            logger.info("ServerClient setting data: Key='" + key1 + "'");
            serverClient.setData(key1, value1);
            logger.info("Client 2 getting data for Key='" + key1 + "'");
            String retrievedValue1 = client2.getData(key1);
            if (value1.equals(retrievedValue1)) {
                logger.success("Success! Client 2 retrieved value from ServerClient: '" + retrievedValue1 + "'");
            } else {
                logger.error("Failure! Client 2 retrieved unexpected value: '" + retrievedValue1 + "'");
            }

            // client2 sets data, serverClient retrieves
            logger.info("Client 2 setting data: Key='" + key2 + "'");
            client2.setData(key2, value2);
            logger.info("ServerClient getting data for Key='" + key2 + "'");
            String retrievedValue2 = serverClient.getData(key2);
             if (value2.equals(retrievedValue2)) {
                logger.success("Success! ServerClient retrieved value from Client 2: '" + retrievedValue2 + "'");
            } else {
                logger.error("Failure! ServerClient retrieved unexpected value: '" + retrievedValue2 + "'");
            }


            // 4. (Optional) Check client list
            logger.info("Checking client list (via Client 2):");
            String clientList = client2.getClientList();
            logger.info(clientList);
            // Expecting at least two clients here (serverClient's internal connection + client2)

        } catch (IOException e) {
            logger.error("IOException during Redis operations", e);
        } catch (RuntimeException e) {
            logger.error("Runtime exception during test", e);
        } finally {
            // 5. Clean up: Disconnect clients and stop the server
            logger.info("Cleaning up resources...");
            if (client2 != null && client2.isConnected()) {
                client2.disconnect();
                 logger.info("Client 2 disconnected.");
            }
            if (serverClient != null && serverClient.isConnected()) {
                // Disconnecting the instance that manages the embedded server will stop it.
                serverClient.disconnect();
                logger.info("ServerClient disconnected and embedded server stopped.");
            }
        }
    }
}