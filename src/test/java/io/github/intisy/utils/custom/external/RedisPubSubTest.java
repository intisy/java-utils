package io.github.intisy.utils.custom.external;

import io.github.intisy.simple.logger.SimpleLogger; // Assuming you have a logger implementation
import io.github.intisy.utils.utils.ThreadUtils; // Using your ThreadUtils for convenience

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisPubSubTest {

    private static final String TEST_CHANNEL = "test-notifications";

    public static void main(String[] args) {
        SimpleLogger logger = new SimpleLogger(); // Use your actual logger
        Redis serverClient = null; // This instance will manage the embedded server AND publish
        Redis listenerClient = null; // This instance will subscribe and listen

        // Use CountDownLatch to wait for messages in the test
        final int expectedMessages = 3;
        CountDownLatch messageLatch = new CountDownLatch(expectedMessages);
        AtomicInteger receivedMessages = new AtomicInteger(0);


        // Define the listener logic
        Redis.MessageListener messageListener = (channel, message) -> {
            logger.info("[Listener] Received message on channel '" + channel + "': " + message);
            receivedMessages.incrementAndGet();
            messageLatch.countDown(); // Signal that a message was received
        };

        Thread listenerThread = null;

        try {
            // 1. Start the embedded server using serverClient
            logger.info("Creating server+client instance and starting embedded Redis...");
            serverClient = new Redis("localhost", 6379, true, true, false);
            serverClient.setLogger(logger);
            serverClient.connect();

            if (!serverClient.isConnected() || !serverClient.ping()) {
                 throw new RuntimeException("Failed to start or connect the embedded Redis server instance.");
            }
            logger.success("Embedded Redis server started by serverClient on host: " + serverClient.getHost() + ", port: " + serverClient.getPort());

            // 2. Create and connect the listener client
            logger.info("Creating Listener client...");
            listenerClient = new Redis(serverClient.getHost(), serverClient.getPort(), false, false, false);
            listenerClient.setLogger(logger);
            listenerClient.connect();

            if (!listenerClient.isConnected() || !listenerClient.ping()) {
                 throw new RuntimeException("Listener client failed to connect.");
            }
            logger.success("Listener client connected successfully.");

            // 3. Start subscribing in a separate thread
            logger.info("Starting listener thread to subscribe to channel: " + TEST_CHANNEL);
            final Redis finalListenerClient = listenerClient; // Need final variable for lambda
            listenerThread = ThreadUtils.newThread(() -> {
                 try {
                    // This call will likely block until the listenerClient is disconnected
                    finalListenerClient.subscribe(TEST_CHANNEL, messageListener);
                    logger.info("[Listener Thread] Subscription ended.");
                 } catch (Exception e) {
                    // Catch redis.clients.jedis.exceptions.JedisConnectionException if disconnected externally
                    if (e.getCause() instanceof java.net.SocketException && e.getCause().getMessage().contains("Socket closed")) {
                         logger.warn("[Listener Thread] Subscription interrupted by disconnection, which is expected on shutdown.");
                    } else {
                         logger.error("[Listener Thread] Error during subscription", e);
                    }
                 }
            }, "redis-listener-thread");

            // Give the listener thread a moment to establish the subscription
            logger.info("Waiting briefly for listener to initialize...");
            ThreadUtils.sleep(1000); // Allow 1 second for subscription setup

            // 4. Publish messages from the serverClient
            logger.info("ServerClient publishing messages to channel: " + TEST_CHANNEL);
            for (int i = 1; i <= expectedMessages; i++) {
                String message = "Message " + i + " from ServerClient";
                logger.info("[Publisher] Sending: " + message);
                serverClient.publish(TEST_CHANNEL, message);
                ThreadUtils.sleep(200); // Small delay between messages
            }

            // 5. Wait for messages to be received or timeout
            logger.info("Waiting for listener to receive " + expectedMessages + " messages...");
            boolean messagesReceived = messageLatch.await(5, TimeUnit.SECONDS); // Wait up to 5 seconds

            if (messagesReceived) {
                logger.success("Success! Listener received all " + expectedMessages + " expected messages.");
            } else {
                logger.error("Failure! Listener only received " + receivedMessages.get() + " out of " + expectedMessages + " messages within the timeout.");
            }


        } catch (IOException e) {
            logger.error("IOException during Redis operations", e);
        } catch (InterruptedException e) {
             logger.error("Main thread interrupted", e);
             Thread.currentThread().interrupt(); // Restore interrupted status
        } catch (RuntimeException e) {
            logger.error("Runtime exception during test", e);
        } finally {
            // 6. Clean up
            logger.info("Cleaning up resources...");

            // Disconnect the listener first. This should interrupt the blocking subscribe() call.
            if (listenerClient != null && listenerClient.isConnected()) {
                listenerClient.disconnect();
                 logger.info("Listener client disconnected.");
            }

            // Wait for the listener thread to finish (optional but good practice)
            if (listenerThread != null) {
                try {
                    logger.info("Waiting for listener thread to terminate...");
                    listenerThread.join(2000); // Wait up to 2 seconds
                    if (listenerThread.isAlive()) {
                         logger.warn("Listener thread did not terminate gracefully.");
                    } else {
                         logger.info("Listener thread terminated.");
                    }
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for listener thread to join", e);
                    Thread.currentThread().interrupt();
                }
            }

            // Disconnect the serverClient (which also stops the embedded server)
            if (serverClient != null && serverClient.isConnected()) {
                serverClient.disconnect();
                logger.info("ServerClient disconnected and embedded server stopped.");
            }
        }
    }
}