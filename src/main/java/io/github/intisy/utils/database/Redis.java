package io.github.intisy.utils.database;

import io.github.intisy.utils.core.MapUtils;
import io.github.intisy.utils.log.Log;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

@SuppressWarnings("unused")
public class Redis {
    public interface RedisDataListener {
        void onDataReceived(String key, String value);
        default void onDataSet(String key, String value) {}
    }

    private final List<RedisDataListener> dataListeners = new CopyOnWriteArrayList<>();
    private static final boolean DEFAULT_USE_EMBEDDED = false;
    private static final boolean DEFAULT_ALLOW_PORT_SEARCH = false;
    private static final boolean DEFAULT_ALLOW_MOCK_FALLBACK = false;
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final String DEFAULT_REDIS_HOST = "localhost";

    private final String host;
    private final int port;
    private JedisPool jedisPool;
    private boolean connected;
    private RedisServer embeddedServer;
    private final boolean allowPortSearch;
    private final boolean allowMockFallback;
    private boolean useEmbedded;
    private boolean useMockFallback;
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
        this(host, port, useEmbedded, DEFAULT_ALLOW_PORT_SEARCH);
    }

    public Redis(String host, int port, boolean useEmbedded, boolean allowPortSearch) {
        this(host, port, useEmbedded, allowPortSearch, DEFAULT_ALLOW_MOCK_FALLBACK);
    }

    public Redis(String host, int port, boolean useEmbedded, boolean allowPortSearch, boolean allowMockFallback) {
        this.host = host;
        this.port = port;
        this.useEmbedded = useEmbedded;
        this.allowPortSearch = allowPortSearch;
        this.allowMockFallback = allowMockFallback;
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
                if (!allowMockFallback) {
                    throw e;
                }

                Log.warn("Failed to start embedded Redis server. Falling back to mock implementation.", e);
                mockRedis = new MockRedis();
                mockRedis.startServer();

                for (RedisDataListener listener : dataListeners) {
                    mockRedis.addDataListener(listener);
                }

                useMockFallback = true;
                connected = true;
                Log.info("Using mock Redis implementation as fallback");
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
                poolConfig.setMaxWait(Duration.ofMillis(10000));

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
                    Log.info("Connected to Redis server at " + connectHost + ":" + connectPort);
                }
            } catch (JedisConnectionException e) {
                connected = false;
                Log.error("Failed to connect to Redis server at " + host + ":" + port);
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
                Log.info("Already connected to Redis server at " + host + ":" + port);
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
                if (!allowPortSearch) {
                    Log.warn("Port " + serverPort + " is not available for embedded Redis server and port search is disabled");
                    throw new IOException("Specified port " + serverPort + " is not available and port search is disabled");
                }

                Log.warn("Port " + serverPort + " is not available for embedded Redis server. Trying to find a free port...");
                serverPort = findFreePort();
                if (serverPort == -1) {
                    Log.error("Could not find a free port for embedded Redis server");
                    throw new IOException("Could not find a free port for embedded Redis server");
                }
                Log.info("Found free port for embedded Redis server: " + serverPort);
            }

            try {
                embeddedServer = RedisServer.newRedisServer()
                        .port(serverPort)
                        .setting("bind 127.0.0.1")
                        .setting("daemonize no")
                        .setting("appendonly no")
                        .build();

                embeddedServer.start();
                Log.info("Embedded Redis server started on port: " + serverPort);
            } catch (IOException e) {
                Log.error("Failed to start embedded Redis server on port " + serverPort + ": " + e.getMessage());
                stopEmbeddedServer();
                throw e;
            } catch (Exception e) {
                Log.error("An unexpected error occurred while starting embedded Redis server on port " + serverPort + ": " + e.getMessage(), e);
                stopEmbeddedServer();
                throw new IOException("Unexpected error starting Redis: " + e.getMessage(), e);
            }
        } else {
            Log.info("Embedded Redis server is already running on port: " + port);
        }
    }

    private void stopEmbeddedServer() {
        if (embeddedServer != null && embeddedServer.isActive()) {
            try {
                embeddedServer.stop();
                Log.info("Embedded Redis server stopped on port: " +  port);
            } catch (Exception e) {
                Log.error("Error while stopping embedded Redis server on port " + port, e);
            } finally {
                embeddedServer = null;
            }
        } else if (embeddedServer == null) {
            Log.info("Embedded Redis server was not running or already stopped.");
        }
        if (embeddedServer != null && !embeddedServer.isActive()){
            embeddedServer = null;
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean ping() {
        if (useMockFallback && mockRedis != null) {
            return mockRedis.isRunning();
        }
        if (useEmbedded && embeddedServer == null && !useMockFallback) {
            Log.warn("Cannot ping, embedded server is not running and mock fallback is disabled.");
            return false;
        }
        if (jedisPool == null && !useEmbedded) {
            Log.warn("Cannot ping, JedisPool is not initialized.");
            return false;
        }

        if (useEmbedded && embeddedServer != null && embeddedServer.isActive()) {
            return true;
        } else if (useEmbedded) {
            return false;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            String reply = jedis.ping();
            boolean success = "PONG".equalsIgnoreCase(reply);
            if (!success) {
                Log.warn("Ping failed, received unexpected reply: " + reply);
            } else {
                this.connected = true;
            }
            return success;
        } catch (JedisConnectionException e) {
            Log.warn("Could not connect to Redis server at " + host + ":" + port + " for ping: " + e.getMessage());
            this.connected = false;
            return false;
        } catch (Exception e) {
            Log.warn("An unexpected error occurred during ping: " + e.getMessage(), e);
            this.connected = false;
            return false;
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
                Log.info("Disconnected from Redis server at " + host + ":" + port);
            } catch (Exception e) {
                Log.error("Error while closing Jedis pool", e);
            } finally {
                jedisPool = null;
                connected = false;
            }
        } else {
            Log.info("Not connected to any Redis server");
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

    public String setDataWithExpiry(String key, String value, long seconds) {
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

    public void setUseMockFallback(boolean useMockFallback) {
        this.useMockFallback = useMockFallback;
    }

    public void setUseEmbedded(boolean useEmbedded) {
        this.useEmbedded = useEmbedded;
    }

    public void publish(String channel, String message) {
        if (useMockFallback) {
            mockRedis.publish(channel, message);
            return;
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot publish message.");
            return;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(channel, message);
        } catch (JedisConnectionException e) {
            Log.error("Jedis connection error during PUBLISH operation", e);
            connected = false;
        } catch (Exception e) {
            Log.error("Error publishing message to Redis", e);
        }
    }

    public void subscribe(String channel, MessageListener messageListener) {
        if (useMockFallback) {
            mockRedis.subscribe(channel, messageListener);
            return;
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot subscribe to channel.");
            return;
        }

        new Thread(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        messageListener.onMessage(channel, message);
                    }
                }, channel);
            } catch (JedisConnectionException e) {
                Log.error("Jedis connection error during SUBSCRIBE operation", e);
                connected = false;
            } catch (Exception e) {
                Log.error("Error subscribing to Redis channel", e);
            }
        }, "Redis-Subscriber-" + channel).start();
    }

    public Long enqueue(String queueName, String message) {
        if (useMockFallback) {
            return mockRedis.enqueue(queueName, message);
        }
        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot enqueue message.");
            return -1L;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.rpush(queueName, message);
        } catch (Exception e) {
            Log.error("Error enqueueing message to Redis queue " + queueName, e);
            return -1L;
        }
    }

    public String dequeue(String queueName) {
        if (useMockFallback) {
            return mockRedis.dequeue(queueName);
        }
        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot dequeue message.");
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lpop(queueName);
        } catch (Exception e) {
            Log.error("Error dequeuing message from Redis", e);
            return null;
        }
    }

    public void registerQueueConsumer(String queueName, QueueMessageConsumer consumer) {
        if (useMockFallback) {
            mockRedis.registerQueueConsumer(queueName, consumer);
            return;
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot register queue consumer.");
            return;
        }

        new Thread(() -> {
            try {
                while (isConnected()) {
                    try (Jedis jedis = jedisPool.getResource()) {
                        List<String> result = jedis.blpop(0, queueName);
                        if (result != null && result.size() >= 2) {
                            String queue = result.get(0);
                            String message = result.get(1);
                            consumer.onMessageReceived(queue, message);
                        }
                    }
                }
            } catch (JedisConnectionException e) {
                Log.error("Jedis connection error during BLPOP operation", e);
                connected = false;
            } catch (Exception e) {
                Log.error("Error consuming from Redis queue", e);
            }
        }, "Redis-Queue-Consumer-" + queueName).start();
    }

    public void registerReliableQueueConsumer(String queueName, QueueMessageConsumer consumer) {
        if (useMockFallback) {
            mockRedis.registerQueueConsumer(queueName, consumer); // Mock uses simple consumer
            return;
        }
        if (!isConnected()) {
            Log.error("Not connected to Redis. Cannot register queue consumer for " + queueName);
            return;
        }

        final String processingQueueName = queueName + ":processing:" + UUID.randomUUID().toString();

        new Thread(() -> {
            try {
                while (isConnected()) {
                    try (Jedis jedis = jedisPool.getResource()) {
                        String message = jedis.brpoplpush(queueName, processingQueueName, 0);

                        if (message != null) {
                            try {
                                consumer.onMessageReceived(queueName, message);
                                jedis.lrem(processingQueueName, -1, message);
                            } catch (Exception e) {
                                Log.error("Worker failed to process message. It remains in " + processingQueueName + " for recovery.", e);
                            }
                        }
                    }
                }
            } catch (JedisConnectionException e) {
                Log.error("Redis connection lost for consumer on queue " + queueName, e);
                connected = false;
            } catch (Exception e) {
                Log.error("Error in reliable queue consumer for " + queueName, e);
            }
        }, "Reliable-Queue-Consumer-" + queueName).start();
    }

    public long getQueueLength(String queueName) {
        if (useMockFallback) {
            return mockRedis.getQueueLength(queueName);
        }
        if (!isConnected()) return -1;
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.llen(queueName);
        } catch (Exception e) {
            Log.error("Error getting queue length for " + queueName, e);
            return -1;
        }
    }

    public Long getPositionInQueue(String queueName, String message) {
        if (useMockFallback) {
            return mockRedis.getPositionInQueue(queueName, message);
        }
        if (!isConnected()) return null;
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lpos(queueName, message);
        } catch (Exception e) {
            Log.error("Error getting position in queue for " + queueName, e);
            return null;
        }
    }


    public interface MessageListener {
        void onMessage(String channel, String message);
    }

    public interface QueueMessageConsumer {
        void onMessageReceived(String queueName, String message);
    }

    public static class MockRedis extends Redis {
        private final Map<String, String> dataStore = new HashMap<>();
        private boolean running = false;
        private final Map<String, List<MessageListener>> subscribers = new HashMap<>();
        private final Map<String, List<String>> messageQueues = new HashMap<>();
        private final Map<String, List<QueueMessageConsumer>> queueConsumers = new HashMap<>();

        public MockRedis() {
            super("mock", 0);
        }

        public void startServer() {
            running = true;
            Log.info("Mock Redis server started");
        }

        public void stopServer() {
            running = false;
            dataStore.clear();
            Log.info("Mock Redis server stopped");
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
        public String setDataWithExpiry(String key, String value, long seconds) {
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

        public void publish(String channel, String message) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot publish message.");
                return;
            }

            List<MessageListener> channelSubscribers = subscribers.get(channel);
            if (channelSubscribers != null) {
                for (MessageListener listener : channelSubscribers) {
                    listener.onMessage(channel, message);
                }
            }
        }

        public void subscribe(String channel, MessageListener messageListener) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot subscribe to channel.");
                return;
            }

            subscribers.computeIfAbsent(channel, k -> new CopyOnWriteArrayList<>())
                    .add(messageListener);
        }

        public Long enqueue(String queueName, String message) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot enqueue message.");
                return -1L;
            }

            List<String> queue = messageQueues.computeIfAbsent(queueName, k -> new CopyOnWriteArrayList<>());
            queue.add(message);

            List<QueueMessageConsumer> consumers = queueConsumers.get(queueName);
            if (consumers != null && !consumers.isEmpty()) {
                QueueMessageConsumer consumer = consumers.get(0);
                if (!queue.isEmpty()) {
                    String msg = queue.remove(0);
                    new Thread(() -> consumer.onMessageReceived(queueName, msg)).start();
                }
            }
            return (long) queue.size();
        }

        public String dequeue(String queueName) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot dequeue message.");
                return null;
            }

            List<String> queue = messageQueues.get(queueName);
            if (queue == null || queue.isEmpty()) {
                return null;
            }

            return queue.remove(0);
        }

        public void registerQueueConsumer(String queueName, QueueMessageConsumer consumer) {
            if (!isRunning()) {
                Log.error("Mock Redis server is not running. Cannot register queue consumer.");
                return;
            }

            queueConsumers.computeIfAbsent(queueName, k -> new CopyOnWriteArrayList<>())
                    .add(consumer);

            List<String> queue = messageQueues.get(queueName);
            if (queue != null && !queue.isEmpty()) {
                String message = queue.remove(0);
                new Thread(() -> consumer.onMessageReceived(queueName, message)).start();
            }
        }

        @Override
        public long getQueueLength(String queueName) {
            if (!isRunning()) return -1;
            return messageQueues.getOrDefault(queueName, new ArrayList<>()).size();
        }

        @Override
        public Long getPositionInQueue(String queueName, String message) {
            if (!isRunning()) return null;
            List<String> queue = messageQueues.get(queueName);
            if (queue == null) return null;
            long index = queue.indexOf(message);
            return index == -1 ? null : index;
        }
    }

    public Map<String, String> getDebugInfo() {
        if (useMockFallback) {
            Map<String, String> mockInfo = new HashMap<>();
            mockInfo.put("type", "mock");
            mockInfo.put("status", mockRedis.isRunning() ? "running" : "stopped");
            mockInfo.put("dataStoreSize", String.valueOf(mockRedis.dataStore.size()));
            mockInfo.put("subscribersCount", String.valueOf(mockRedis.subscribers.size()));
            mockInfo.put("queuesCount", String.valueOf(mockRedis.messageQueues.size()));
            mockInfo.put("queueConsumersCount", String.valueOf(mockRedis.queueConsumers.size()));
            return mockInfo;
        }

        if (!isConnected()) {
            Map<String, String> disconnectedInfo = new HashMap<>();
            disconnectedInfo.put("status", "disconnected");
            disconnectedInfo.put("host", host);
            disconnectedInfo.put("port", String.valueOf(port));
            return disconnectedInfo;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> info = new HashMap<>();
            info.put("type", useEmbedded ? "embedded" : "external");
            info.put("status", "connected");
            info.put("host", host);
            info.put("port", String.valueOf(port));
            info.put("clientList", getClientList());
            info.put("dbSize", String.valueOf(jedis.dbSize()));
            info.put("serverInfo", jedis.info());
            info.put("activeListeners", String.valueOf(dataListeners.size()));
            return info;
        } catch (Exception e) {
            Log.error("Error getting debug info", e);
            return MapUtils.newHashMap("error", e.getMessage());
        }
    }

    public String getClientList() {
        if (useMockFallback) {
            return "Mock Redis - no real clients";
        }

        if (!isConnected()) {
            return "Not connected to Redis server";
        }

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.clientList();
        } catch (Exception e) {
            Log.error("Error getting client list", e);
            return "Error: " + e.getMessage();
        }
    }

    public void printDebugStatus() {
        Map<String, String> debugInfo = getDebugInfo();
        Log.info("=== Redis Debug Status ===");
        debugInfo.forEach((key, value) -> {
            if (key.equals("serverInfo")) {
                Log.info("Server Info:");
                String[] infoLines = value.split("\n");
                for (String line : infoLines) {
                    if (!line.trim().isEmpty()) {
                        Log.info("  " + line);
                    }
                }
            } else {
                Log.info(key + ": " + value);
            }
        });
        Log.info("======================");
    }

    public Map<String, Long> getKeyspaceStats() {
        if (useMockFallback) {
            return MapUtils.newHashMap("total_keys", (long) mockRedis.dataStore.size());
        }

        if (!isConnected()) {
            return MapUtils.newHashMap("error", -1L);
        }

        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Long> stats = new HashMap<>();
            stats.put("total_keys", jedis.dbSize());

            Set<String> keys = jedis.keys("*");
            stats.put("string_keys", keys.stream()
                    .filter(k -> jedis.type(k).equals("string"))
                    .count());
            stats.put("list_keys", keys.stream()
                    .filter(k -> jedis.type(k).equals("list"))
                    .count());
            stats.put("set_keys", keys.stream()
                    .filter(k -> jedis.type(k).equals("set"))
                    .count());
            stats.put("hash_keys", keys.stream()
                    .filter(k -> jedis.type(k).equals("hash"))
                    .count());

            return stats;
        } catch (Exception e) {
            Log.error("Error getting keyspace stats", e);
            return MapUtils.newHashMap("error", -1L);
        }
    }

    public void monitorCommands() {
        if (useMockFallback) {
            Log.info("Command monitoring not available in mock mode");
            return;
        }

        if (!isConnected()) {
            Log.error("Not connected to Redis server. Cannot monitor commands.");
            return;
        }

        new Thread(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                Log.info("Starting Redis command monitor...");
                jedis.monitor(new JedisMonitor() {
                    @Override
                    public void onCommand(String command) {
                        Log.info("Redis Command: " + command);
                    }
                });
            } catch (Exception e) {
                Log.error("Error in command monitor", e);
            }
        }, "Redis-Monitor").start();
    }
}