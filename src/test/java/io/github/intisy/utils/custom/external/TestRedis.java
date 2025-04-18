package io.github.intisy.utils.custom.external;

import io.github.intisy.simple.logger.Log;

import java.io.IOException;

/**
 * @author Finn Birich
 */
public class TestRedis {
    public static void main(String[] args) {
        System.out.println("Redis Client Event System Demonstration");
        System.out.println("======================================\n");

        System.out.println("1. Testing with embedded Redis server (with automatic fallback):\n");
        testWithEmbeddedRedis();

        System.out.println("\n2. Testing with explicit Mock Redis (no server required):\n");
        testWithMockRedis();

        System.out.println("\n3. Testing with Kubernetes Redis connection:\n");
        System.out.println("(This will likely fail if not running in Kubernetes or with port forwarding)");
        testWithKubernetesRedis();
    }

    private static void testWithEmbeddedRedis() {
        Redis redis = new Redis(6379, true);

        try {
            System.out.println("Attempting to connect to embedded Redis server...");
            if (redis.isPortAvailable(redis.getPort()))
                redis.connect();

            if (redis.isConnected()) {
                System.out.println("Embedded Redis server is running");

                redis.addDataListener(new Redis.RedisDataListener() {
                    @Override
                    public void onDataReceived(String key, String value) {
                        System.out.println("[EVENT] Data received - Key: " + key + ", Value: " + value);
                    }

                    @Override
                    public void onDataSet(String key, String value) {
                        System.out.println("[EVENT] Data set - Key: " + key + ", Value: " + value);
                    }
                });

                String key = "testKey";
                String value = "testValue";

                System.out.println("\nTesting Redis operations:");
                System.out.println("Setting data: " + key + " = " + value);
                redis.setData(key, value);

                System.out.println("Getting data for key: " + key);
                String retrievedValue = redis.getData(key);
                System.out.println("Retrieved value: " + retrievedValue);

                System.out.println("Checking if key exists: " + key);
                boolean keyExists = redis.exists(key);
                System.out.println("Key exists: " + keyExists);

                System.out.println("Setting data with expiry: expireKey = expireValue (10 seconds)");
                redis.setDataWithExpiry("expireKey", "expireValue", 10);

                System.out.println("Deleting key: " + key);
                redis.deleteData(key);

                System.out.println("Checking if key exists after deletion: " + key);
                boolean keyExistsAfterDeletion = redis.exists(key);
                System.out.println("Key exists after deletion: " + keyExistsAfterDeletion);
            } else {
                System.out.println("Failed to connect to Redis server.");
            }
        } catch (IOException e) {
            System.out.println("Failed to start embedded Redis: " + e.getMessage());
        } finally {
            redis.disconnect();
        }
    }

    private static void testWithMockRedis() {
        Redis.MockRedis mockRedis = new Redis.MockRedis();

        try {
            mockRedis.connect();

            if (mockRedis.isConnected()) {
                System.out.println("Mock Redis server is running");

                mockRedis.addDataListener(new Redis.RedisDataListener() {
                    @Override
                    public void onDataReceived(String key, String value) {
                        System.out.println("[EVENT] Data received - Key: " + key + ", Value: " + value);
                    }

                    @Override
                    public void onDataSet(String key, String value) {
                        System.out.println("[EVENT] Data set - Key: " + key + ", Value: " + value);
                    }
                });

                String key = "myKey";
                String value = "myValue";

                mockRedis.setData(key, value);
                mockRedis.getData(key);
                mockRedis.exists(key);
                mockRedis.setDataWithExpiry("tempKey", "This will expire (in a real Redis)", 10);
                mockRedis.deleteData(key);

                mockRedis.addDataListener((key1, value1) -> System.out.println("[EVENT - Listener 2] Data received - Key: " + key1 + ", Value: " + value1));

                mockRedis.getData("tempKey");
            }
        } catch(Exception e) {
            Log.error("An unexpected error occurred in mock test: ", e);
        } finally {
            mockRedis.disconnect();
        }
    }

    private static void testWithKubernetesRedis() {
        Redis redis = new Redis("redis-service.default.svc.cluster.local", 6379);

        try {
            System.out.println("Connecting to Redis at " + redis.getHost() + ":" + redis.getPort());
            redis.connect();

            if (redis.isConnected()) {
                System.out.println("Connected to Redis server");

                redis.addDataListener(new Redis.RedisDataListener() {
                    @Override
                    public void onDataReceived(String key, String value) {
                        System.out.println("[EVENT] Data received - Key: " + key + ", Value: " + value);
                    }

                    @Override
                    public void onDataSet(String key, String value) {
                        System.out.println("[EVENT] Data set - Key: " + key + ", Value: " + value);
                    }
                });

                String key = "myKey";
                String value = "myValue";

                redis.setData(key, value);
                redis.getData(key);

                redis.setData("user:1:name", "John Doe");
                redis.setData("user:1:email", "john@example.com");
                redis.getData("user:1:name");
                redis.getData("user:1:email");

                redis.exists("user:1:name");
                redis.setDataWithExpiry("session:123", "user-session-data", 60);
                redis.deleteData("user:1:name");
            } else {
                System.out.println("Failed to connect to Redis server.");
            }
        } catch (IOException e) {
            System.out.println("Failed to connect to Redis: " + e.getMessage());
        } finally {
            redis.disconnect();
        }
    }
}
