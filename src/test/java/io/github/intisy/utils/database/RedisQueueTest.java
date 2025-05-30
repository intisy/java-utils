package io.github.intisy.utils.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisQueueTest {
    
    private static final String QUEUE_NAME = "test_work_queue";
    private static final int NUMBER_OF_WORKERS = 5;
    private static final int NUMBER_OF_MESSAGES = 20;
    private static final int MIN_PROCESSING_TIME_MS = 500;
    private static final int MAX_PROCESSING_TIME_MS = 2000;
    
    // Track which worker processed each message
    private static final AtomicInteger[] messageProcessedByWorker = new AtomicInteger[NUMBER_OF_WORKERS];
    
    // Track which sender sent each message
    private static final Map<String, Integer> messageSourceMap = new ConcurrentHashMap<>();
    private static final AtomicInteger[] messagesBySender = new AtomicInteger[2];
    
    static {
        for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
            messageProcessedByWorker[i] = new AtomicInteger(0);
        }
        
        for (int i = 0; i < messagesBySender.length; i++) {
            messagesBySender[i] = new AtomicInteger(0);
        }
    }
    
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void main(String[] args) {
        List<Redis> redisInstances = new ArrayList<>();
        
        try {
            // Start a main Redis server that will be shared
            Redis redisServer = new Redis("localhost", 6379, true, true, false);
            redisServer.connect();
            redisInstances.add(redisServer);
            System.out.println("Redis server started on port: " + redisServer.getPort());
            
            // CountDownLatch to wait for all messages to be processed
            CountDownLatch latch = new CountDownLatch(NUMBER_OF_MESSAGES);
            
            // Create and start workers, each with its own Redis client
            for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                // Create a Redis client for this worker (connecting to the shared server)
                Redis workerRedis = new Redis(redisServer.getHost(), redisServer.getPort(), false, false, false);
                workerRedis.connect();
                redisInstances.add(workerRedis);
                
                Worker worker = new Worker(i, workerRedis, latch);
                Thread workerThread = new Thread(worker);
                workerThread.setName("Worker-" + i);
                workerThread.start();
            }
            
            // Give workers time to register
            Thread.sleep(1000);
            
            // Create two senders with their own Redis clients
            Redis senderRedis1 = new Redis(redisServer.getHost(), redisServer.getPort(), false, false, false);
            senderRedis1.connect();
            redisInstances.add(senderRedis1);
            
            Redis senderRedis2 = new Redis(redisServer.getHost(), redisServer.getPort(), false, false, false);
            senderRedis2.connect();
            redisInstances.add(senderRedis2);
            
            Sender sender1 = new Sender(0, senderRedis1);
            Sender sender2 = new Sender(1, senderRedis2);
            
            // Send messages, alternating between senders
            for (int i = 1; i <= NUMBER_OF_MESSAGES; i++) {
                String message = "Message " + i;
                
                // Alternate between senders
                if (i % 2 == 0) {
                    sender1.sendMessage(message);
                    messageSourceMap.put(message, 0); // Track sender ID
                } else {
                    sender2.sendMessage(message);
                    messageSourceMap.put(message, 1); // Track sender ID
                }
                
                // Small delay between sends to make output more readable
                Thread.sleep(100);
            }
            
            // Wait for all messages to be processed
            System.out.println("Waiting for all messages to be processed...");
            latch.await(30, TimeUnit.SECONDS);
            
            // Print statistics
            System.out.println("\n--- Processing Statistics ---");
            for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                System.out.println("Worker " + i + " processed " + messageProcessedByWorker[i].get() + " messages");
            }
            
            System.out.println("\n--- Sender Statistics ---");
            System.out.println("Sender 1 sent " + messagesBySender[0].get() + " messages");
            System.out.println("Sender 2 sent " + messagesBySender[1].get() + " messages");
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Cleanup - disconnect all Redis instances
            for (Redis redis : redisInstances) {
                try {
                    if (redis != null && redis.isConnected()) {
                        redis.disconnect();
                    }
                } catch (Exception e) {
                    System.err.println("Error disconnecting Redis instance: " + e.getMessage());
                }
            }
            System.out.println("All Redis instances disconnected.");
        }
    }
    
    /**
     * Sender class that enqueues messages to the Redis queue
     */
    static class Sender {
        private final int id;
        private final Redis redis;
        
        public Sender(int id, Redis redis) {
            this.id = id;
            this.redis = redis;
        }
        
        public void sendMessage(String message) {
            boolean success = redis.enqueue(QUEUE_NAME, message);
            messagesBySender[id].incrementAndGet();
            System.out.println("Sender " + id + ": Enqueued message '" + message + "' - Success: " + success);
        }
    }
    
    /**
     * Worker class that consumes messages from the Redis queue
     * and simulates processing with a delay
     */
    static class Worker implements Runnable, Redis.QueueMessageConsumer {
        private final int id;
        private final Redis redis;
        private final CountDownLatch latch;
        private final Random random = new Random();
        private boolean processing = false;
        
        public Worker(int id, Redis redis, CountDownLatch latch) {
            this.id = id;
            this.redis = redis;
            this.latch = latch;
        }
        
        @Override
        public void run() {
            try {
                System.out.println("Worker " + id + " started with Redis connection and registered to queue " + QUEUE_NAME);
                redis.registerQueueConsumer(QUEUE_NAME, this);
            } catch (Exception e) {
                System.err.println("Error starting Worker " + id + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        @Override
        public void onMessageReceived(String queueName, String message) {
            // Skip if already processing (simulating that a worker can only handle one task at a time)
            if (processing) {
                // Re-enqueue the message so another worker can handle it
                System.out.println("Worker " + id + " is busy, re-enqueueing message: " + message);
                redis.enqueue(queueName, message);
                return;
            }
            
            processing = true;
            
            try {
                // Increment the counter for this worker
                messageProcessedByWorker[id].incrementAndGet();
                
                // Get the sender ID from the message source map
                Integer senderId = messageSourceMap.getOrDefault(message, -1);
                
                // Random processing time between MIN and MAX
                int processingTime = MIN_PROCESSING_TIME_MS + 
                                     random.nextInt(MAX_PROCESSING_TIME_MS - MIN_PROCESSING_TIME_MS);
                
                System.out.println("Worker " + id + " processing message: '" + message + 
                                   "' from Sender " + senderId +
                                   " (will take " + processingTime + "ms)");
                
                // Simulate processing time
                Thread.sleep(processingTime);
                
                System.out.println("Worker " + id + " completed processing message: '" + message + 
                                   "' from Sender " + senderId);
                
                // Count down the latch to indicate message has been processed
                latch.countDown();
                
            } catch (InterruptedException e) {
                System.err.println("Worker " + id + " was interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
            } finally {
                // Mark as no longer processing
                processing = false;
            }
        }
    }
}
