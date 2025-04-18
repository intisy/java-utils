package io.github.intisy.utils.custom.external;

import io.github.intisy.simple.logger.Log;
import redis.embedded.RedisServer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@SuppressWarnings("unused")
public class Redis {
    public interface RedisDataListener {
        void onDataReceived(String key, String value);
        default void onDataSet(String key, String value) {}
    }

    private final List<RedisDataListener> dataListeners = new CopyOnWriteArrayList<>();
    private static final boolean DEFAULT_USE_EMBEDDED = false;
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final String DEFAULT_REDIS_HOST = "localhost";

    private final String host;
    private final int port;
    private final boolean useEmbedded;
    private JedisPool jedisPool;
    private boolean connected = false;
    private RedisServer embeddedServer;
    private boolean useMockFallback = false;
    private MockRedis mockRedis;

    public Redis() {
        this(DEFAULT_REDIS_HOST, DEFAULT_REDIS_PORT, DEFAULT_USE_EMBEDDED);
    }

    public Redis(int port) {
        this(DEFAULT_REDIS_HOST, port, DEFAULT_USE_EMBEDDED);
    }

    public Redis(boolean useEmbedded) {
        this(DEFAULT_REDIS_HOST, DEFAULT_REDIS_PORT, useEmbedded);
    }

    public Redis(int port, boolean useEmbedded) {
        this(DEFAULT_REDIS_HOST, port, useEmbedded);
    }

    public Redis(String host, int port) {
        this(host, port, DEFAULT_USE_EMBEDDED);
    }

    public Redis(String host, int port, boolean useEmbedded) {
        this.host = host;
        this.port = port;
        this.useEmbedded = useEmbedded;
    }

    public boolean isPortAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            return -1;
        }
    }

    public void connect() throws IOException {
        if (useEmbedded) {
            try {
                startEmbeddedServer();
            } catch (IOException e) {
                Log.warn("Failed to start embedded Redis server. Falling back to mock implementation.", e);
                mockRedis = new MockRedis();
                mockRedis.startServer();

                for (RedisDataListener listener : dataListeners) {
                    mockRedis.addDataListener(listener);
                }

                useMockFallback = true;
                connected = true;
                Log.note("Using mock Redis implementation as fallback");
                return;
            }
        }

        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }

        if (jedisPool == null || jedisPool.isClosed()) {
            try {
                JedisPoolConfig poolConfig = new JedisPoolConfig();
                poolConfig.setMaxTotal(128);
                poolConfig.setMaxIdle(32);
                poolConfig.setMinIdle(8);
                poolConfig.setTestOnBorrow(true);
                poolConfig.setTestOnReturn(true);
                poolConfig.setTestWhileIdle(true);
                poolConfig.setBlockWhenExhausted(true);
                poolConfig.setMaxWaitMillis(10000);

                String connectHost = host;
                int connectPort = port;
                if (useEmbedded && embeddedServer != null) {
                    connectHost = "localhost";
                    connectPort = embeddedServer.ports().get(0);
                }

                jedisPool = new JedisPool(poolConfig, connectHost, connectPort, 5000);

                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.ping();
                    connected = true;
                    Log.note("Connected to Redis server at " + connectHost + ":" + connectPort);
                }
            } catch (JedisConnectionException e) {
                connected = false;
                Log.error("Failed to connect to Redis server at " + host + ":" + port, e);
                if (jedisPool != null) {
                    jedisPool.close();
                    jedisPool = null;
                }
                throw new IOException("Failed to connect to Redis server: " + e.getMessage(), e);
            }
        } else {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.ping();
                connected = true;
                Log.note("Already connected to Redis server at " + host + ":" + port);
            } catch (JedisConnectionException e) {
                connected = false;
                Log.error("Connection to Redis server at " + host + ":" + port + " is broken", e);
                jedisPool.close();
                jedisPool = null;
                throw new IOException("Connection to Redis server is broken: " + e.getMessage(), e);
            }
        }
    }

    private void startEmbeddedServer() throws IOException {
        if (embeddedServer == null || !embeddedServer.isActive()) {
            int serverPort = port;

            if (!isPortAvailable(serverPort)) {
                Log.warn("Port " + serverPort + " is not available for embedded Redis server. Trying to find a free port...");
                serverPort = findFreePort();
                if (serverPort == -1) {
                    Log.error("Could not find a free port for embedded Redis server");
                    throw new IOException("Could not find a free port for embedded Redis server");
                }
                Log.note("Found free port for embedded Redis server: " + serverPort);
            }

            try {
                embeddedServer = RedisServer.newRedisServer()
                        .port(serverPort)
                        .setting("bind 127.0.0.1")
                        .setting("daemonize no")
                        .setting("appendonly no")
                        .build();

                embeddedServer.start();
                Log.note("Embedded Redis server started on port: " + serverPort);
            } catch (IOException e) {
                Log.error("Failed to start embedded Redis server on port " + serverPort + ": " + e.getMessage(), e);
                stopEmbeddedServer();
                throw e;
            } catch (Exception e) {
                Log.error("An unexpected error occurred while starting embedded Redis server on port " + serverPort + ": " + e.getMessage(), e);
                stopEmbeddedServer();
                throw new IOException("Unexpected error starting Redis: " + e.getMessage(), e);
            }
        } else {
            Log.note("Embedded Redis server is already running on port: " + port);
        }
    }

    private void stopEmbeddedServer() {
        if (embeddedServer != null && embeddedServer.isActive()) {
            try {
                embeddedServer.stop();
                Log.note("Embedded Redis server stopped on port: " +  port);
            } catch (Exception e) {
                Log.error("Error while stopping embedded Redis server on port " + port, e);
            } finally {
                embeddedServer = null;
            }
        } else if (embeddedServer == null) {
            Log.note("Embedded Redis server was not running or already stopped.");
        }
        if (embeddedServer != null && !embeddedServer.isActive()){
            embeddedServer = null;
        }
    }


    public void disconnect() {
        if (useMockFallback && mockRedis != null) {
            mockRedis.stopServer();
            mockRedis = null;
            useMockFallback = false;
            connected = false;
            return;
        }

        if (useEmbedded) {
            stopEmbeddedServer();
        }

        if (jedisPool != null) {
            try {
                jedisPool.close();
                Log.note("Disconnected from Redis server at " + host + ":" + port);
            } catch (Exception e) {
                Log.error("Error while closing Jedis pool", e);
            } finally {
                jedisPool = null;
                connected = false;
            }
        } else {
            Log.note("Not connected to any Redis server");
        }
    }

    public boolean isConnected() {
        if (useMockFallback) {
            return mockRedis != null && mockRedis.isRunning();
        }

        if (useEmbedded && (embeddedServer == null || !embeddedServer.isActive())) {
            return false;
        }

        if (!connected || jedisPool == null || jedisPool.isClosed()) {
            return false;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.ping();
            return true;
        } catch (Exception e) {
            connected = false;
            return false;
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String setData(String key, String value) {
        if (useMockFallback) {
            return mockRedis.setData(key, value);
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot set data.");
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(key, value);
            notifyDataSetListeners(key, value);
            return result;
        } catch (JedisConnectionException e) {
            Log.error("Jedis connection error during SET operation", e);
            connected = false;
            return null;
        } catch (Exception e) {
            Log.error("Error setting data in Redis", e);
            return null;
        }
    }

    public String getData(String key) {
        if (useMockFallback) {
            return mockRedis.getData(key);
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot get data.");
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key);
            notifyDataListeners(key, value);
            return value;
        } catch (JedisConnectionException e) {
            Log.error("Jedis connection error during GET operation", e);
            connected = false;
            return null;
        } catch (Exception e) {
            Log.error("Error getting data from Redis", e);
            return null;
        }
    }

    public Long deleteData(String key) {
        if (useMockFallback) {
            return mockRedis.deleteData(key);
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot delete data.");
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key);
        } catch (JedisConnectionException e) {
            Log.error("Jedis connection error during DELETE operation", e);
            connected = false;
            return null;
        } catch (Exception e) {
            Log.error("Error deleting data from Redis", e);
            return null;
        }
    }

    public boolean exists(String key) {
        if (useMockFallback) {
            return mockRedis.exists(key);
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot check if key exists.");
            return false;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(key);
        } catch (JedisConnectionException e) {
            Log.error("Jedis connection error during EXISTS operation", e);
            connected = false;
            return false;
        } catch (Exception e) {
            Log.error("Error checking if key exists in Redis", e);
            return false;
        }
    }

    public String setDataWithExpiry(String key, String value, int seconds) {
        if (useMockFallback) {
            return mockRedis.setDataWithExpiry(key, value, seconds);
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot set data with expiry.");
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.setex(key, seconds, value);
            notifyDataSetListeners(key, value);
            return result;
        } catch (JedisConnectionException e) {
            Log.error("Jedis connection error during SETEX operation", e);
            connected = false;
            return null;
        } catch (Exception e) {
            Log.error("Error setting data with expiry in Redis", e);
            return null;
        }
    }

    protected void notifyDataListeners(String key, String value) {
        for (RedisDataListener listener : dataListeners) {
            try {
                listener.onDataReceived(key, value);
            } catch (Exception e) {
                Log.error("Error notifying Redis data listener for data received", e);
            }
        }
    }

    protected void notifyDataSetListeners(String key, String value) {
        for (RedisDataListener listener : dataListeners) {
            try {
                listener.onDataSet(key, value);
            } catch (Exception e) {
                Log.error("Error notifying Redis data listener for data set", e);
            }
        }
    }

    public void addDataListener(RedisDataListener listener) {
        if (listener != null && !dataListeners.contains(listener)) {
            dataListeners.add(listener);
        }
    }

    public void removeDataListener(RedisDataListener listener) {
        dataListeners.remove(listener);
    }


    public static class MockRedis extends Redis {
        private final Map<String, String> dataStore = new HashMap<>();
        private boolean running = false;

        public MockRedis() {
            super("mock", 0);
        }

        public void startServer() {
            running = true;
            Log.note("Mock Redis server started");
        }

        public void stopServer() {
            running = false;
            dataStore.clear();
            Log.note("Mock Redis server stopped");
        }

        public boolean isRunning() {
            return running;
        }

        @Override
        public String setData(String key, String value) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot set data.");
                return null;
            }
            dataStore.put(key, value);
            notifyDataSetListeners(key, value);
            return "OK";
        }

        @Override
        public String getData(String key) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot get data.");
                return null;
            }
            String value = dataStore.get(key);
            notifyDataListeners(key, value);
            return value;
        }

        @Override
        public Long deleteData(String key) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot delete data.");
                return null;
            }
            if (dataStore.containsKey(key)) {
                dataStore.remove(key);
                return 1L;
            }
            return 0L;
        }

        @Override
        public boolean exists(String key) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot check if key exists.");
                return false;
            }
            return dataStore.containsKey(key);
        }

        @Override
        public String setDataWithExpiry(String key, String value, int seconds) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot set data with expiry.");
                return null;
            }
            dataStore.put(key, value);
            notifyDataSetListeners(key, value);
            return "OK";
        }

        @Override
        public boolean isConnected() {
            return isRunning();
        }

        @Override
        public void connect() {
            startServer();
        }

        @Override
        public void disconnect() {
            stopServer();
        }
    }
}