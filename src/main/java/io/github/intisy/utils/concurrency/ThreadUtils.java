package io.github.intisy.utils.concurrency;

/**
 * Utility class providing thread-related operations and convenience methods.
 * This class includes methods for thread creation, sleep operations, and thread information retrieval.
 *
 * @author Finn Birich
 */
@SuppressWarnings({"unused", "InfiniteLoopStatement"})
public class ThreadUtils {
    /**
     * Causes the current thread to enter an infinite sleep loop.
     * This method will continuously sleep for 1000 milliseconds (1 second) in an infinite loop.
     * Note: This method will never return and should be used with caution.
     */
    public static void sleep() {
        while (true)
            ThreadUtils.sleep(1000);
    }

    /**
     * Causes the current thread to sleep for the specified number of milliseconds.
     * If the thread is interrupted while sleeping, the method will throw a RuntimeException
     * wrapping the InterruptedException.
     *
     * @param milliseconds the length of time to sleep in milliseconds
     * @throws RuntimeException if the thread is interrupted while sleeping
     */
    public static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates and starts a new thread to run the specified Runnable.
     * The thread will be created without a specific name and as a non-daemon thread.
     *
     * @param runnable the Runnable to be executed in the new thread
     * @return the started Thread object
     */
    public static Thread newThread(Runnable runnable) {
        return newThread(runnable, null);
    }

    /**
     * Creates and starts a new thread with the specified name to run the given Runnable.
     * The thread will be created as a non-daemon thread.
     *
     * @param runnable the Runnable to be executed in the new thread
     * @param name the name to assign to the new thread, or null for a default name
     * @return the started Thread object
     */
    public static Thread newThread(Runnable runnable, String name) {
        return newThread(runnable, name, false);
    }

    /**
     * Creates and starts a new thread with the specified name and daemon status to run the given Runnable.
     *
     * @param runnable the Runnable to be executed in the new thread
     * @param name the name to assign to the new thread, or null for a default name
     * @param daemon whether the thread should be a daemon thread
     * @return the started Thread object
     */
    public static Thread newThread(Runnable runnable, String name, boolean daemon) {
        Thread thread = name != null ? new Thread(runnable, name) : new Thread(runnable);
        thread.setDaemon(daemon);
        thread.start();
        return thread;
    }

    /**
     * Returns the name of the current thread.
     *
     * @return the name of the current thread
     */
    public static String getThreadName() {
        return Thread.currentThread().getName();
    }

    /**
     * Prints the name of the current thread to the standard output.
     * The output format is "Current thread: [thread-name]".
     */
    public static void printThreadName() {
        System.out.println("Current thread: " + ThreadUtils.getThreadName());
    }
}
