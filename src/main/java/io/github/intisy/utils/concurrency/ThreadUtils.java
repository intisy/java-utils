package io.github.intisy.utils.concurrency;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
     * Creates and starts a new thread to execute the specified Consumer with the given input.
     * The thread will be created without a specific name and as a non-daemon thread.
     *
     * @param <T> the type of the input to the consumer
     * @param consumer the Consumer to be executed in the new thread
     * @param input the input to be passed to the consumer
     * @return the started Thread object
     */
    public static <T> Thread newThread(Consumer<T> consumer, T input) {
        return newThread(() -> consumer.accept(input));
    }

    /**
     * Creates and starts a new thread with the specified name to execute the given Consumer with the input.
     * The thread will be created as a non-daemon thread.
     *
     * @param <T> the type of the input to the consumer
     * @param consumer the Consumer to be executed in the new thread
     * @param input the input to be passed to the consumer
     * @param name the name to assign to the new thread, or null for a default name
     * @return the started Thread object
     */
    public static <T> Thread newThread(Consumer<T> consumer, T input, String name) {
        return newThread(() -> consumer.accept(input), name);
    }

    /**
     * Creates and starts a new thread with the specified name and daemon status to execute the given Consumer with the input.
     *
     * @param <T> the type of the input to the consumer
     * @param consumer the Consumer to be executed in the new thread
     * @param input the input to be passed to the consumer
     * @param name the name to assign to the new thread, or null for a default name
     * @param daemon whether the thread should be a daemon thread
     * @return the started Thread object
     */
    public static <T> Thread newThread(Consumer<T> consumer, T input, String name, boolean daemon) {
        return newThread(() -> consumer.accept(input), name, daemon);
    }

    /**
     * Creates and starts a new thread to execute the specified BiConsumer with the given inputs.
     * The thread will be created without a specific name and as a non-daemon thread.
     *
     * @param <T> the type of the first input to the bi-consumer
     * @param <U> the type of the second input to the bi-consumer
     * @param biConsumer the BiConsumer to be executed in the new thread
     * @param input1 the first input to be passed to the bi-consumer
     * @param input2 the second input to be passed to the bi-consumer
     * @return the started Thread object
     */
    public static <T, U> Thread newThread(BiConsumer<T, U> biConsumer, T input1, U input2) {
        return newThread(() -> biConsumer.accept(input1, input2));
    }

    /**
     * Creates and starts a new thread with the specified name to execute the given BiConsumer with the inputs.
     * The thread will be created as a non-daemon thread.
     *
     * @param <T> the type of the first input to the bi-consumer
     * @param <U> the type of the second input to the bi-consumer
     * @param biConsumer the BiConsumer to be executed in the new thread
     * @param input1 the first input to be passed to the bi-consumer
     * @param input2 the second input to be passed to the bi-consumer
     * @param name the name to assign to the new thread, or null for a default name
     * @return the started Thread object
     */
    public static <T, U> Thread newThread(BiConsumer<T, U> biConsumer, T input1, U input2, String name) {
        return newThread(() -> biConsumer.accept(input1, input2), name);
    }

    /**
     * Creates and starts a new thread with the specified name and daemon status to execute the given BiConsumer with the inputs.
     *
     * @param <T> the type of the first input to the bi-consumer
     * @param <U> the type of the second input to the bi-consumer
     * @param biConsumer the BiConsumer to be executed in the new thread
     * @param input1 the first input to be passed to the bi-consumer
     * @param input2 the second input to be passed to the bi-consumer
     * @param name the name to assign to the new thread, or null for a default name
     * @param daemon whether the thread should be a daemon thread
     * @return the started Thread object
     */
    public static <T, U> Thread newThread(BiConsumer<T, U> biConsumer, T input1, U input2, String name, boolean daemon) {
        return newThread(() -> biConsumer.accept(input1, input2), name, daemon);
    }

    /**
     * Creates and starts a new thread to execute the specified Supplier.
     * The thread will be created without a specific name and as a non-daemon thread.
     * Note: The return value of the Supplier is discarded.
     *
     * @param <T> the type of the result supplied by the supplier
     * @param supplier the Supplier to be executed in the new thread
     * @return the started Thread object
     */
    public static <T> Thread newThread(Supplier<T> supplier) {
        return newThread(supplier, null);
    }

    /**
     * Creates and starts a new thread with the specified name to execute the given Supplier.
     * The thread will be created as a non-daemon thread.
     * Note: The return value of the Supplier is discarded.
     *
     * @param <T> the type of the result supplied by the supplier
     * @param supplier the Supplier to be executed in the new thread
     * @param name the name to assign to the new thread, or null for a default name
     * @return the started Thread object
     */
    public static <T> Thread newThread(Supplier<T> supplier, String name) {
        return newThread(supplier, name, false);
    }

    /**
     * Creates and starts a new thread with the specified name and daemon status to execute the given Supplier.
     * Note: The return value of the Supplier is discarded.
     *
     * @param <T> the type of the result supplied by the supplier
     * @param supplier the Supplier to be executed in the new thread
     * @param name the name to assign to the new thread, or null for a default name
     * @param daemon whether the thread should be a daemon thread
     * @return the started Thread object
     */
    public static <T> Thread newThread(Supplier<T> supplier, String name, boolean daemon) {
        return newThread(() -> {
            supplier.get();
        }, name, daemon);
    }

    /**
     * Creates and starts a new thread to execute the specified Function with the given input.
     * The thread will be created without a specific name and as a non-daemon thread.
     * Note: The return value of the Function is discarded.
     *
     * @param <T> the type of the input to the function
     * @param <R> the type of the result of the function
     * @param function the Function to be executed in the new thread
     * @param input the input to be passed to the function
     * @return the started Thread object
     */
    public static <T, R> Thread newThread(Function<T, R> function, T input) {
        return newThread(() -> function.apply(input));
    }

    /**
     * Creates and starts a new thread with the specified name to execute the given Function with the input.
     * The thread will be created as a non-daemon thread.
     * Note: The return value of the Function is discarded.
     *
     * @param <T> the type of the input to the function
     * @param <R> the type of the result of the function
     * @param function the Function to be executed in the new thread
     * @param input the input to be passed to the function
     * @param name the name to assign to the new thread, or null for a default name
     * @return the started Thread object
     */
    public static <T, R> Thread newThread(Function<T, R> function, T input, String name) {
        return newThread(() -> function.apply(input), name);
    }

    /**
     * Creates and starts a new thread with the specified name and daemon status to execute the given Function with the input.
     * Note: The return value of the Function is discarded.
     *
     * @param <T> the type of the input to the function
     * @param <R> the type of the result of the function
     * @param function the Function to be executed in the new thread
     * @param input the input to be passed to the function
     * @param name the name to assign to the new thread, or null for a default name
     * @param daemon whether the thread should be a daemon thread
     * @return the started Thread object
     */
    public static <T, R> Thread newThread(Function<T, R> function, T input, String name, boolean daemon) {
        return newThread(() -> function.apply(input), name, daemon);
    }

    /**
     * Creates and starts a new thread to execute the specified BiFunction with the given inputs.
     * The thread will be created without a specific name and as a non-daemon thread.
     * Note: The return value of the BiFunction is discarded.
     *
     * @param <T> the type of the first input to the bi-function
     * @param <U> the type of the second input to the bi-function
     * @param <R> the type of the result of the bi-function
     * @param biFunction the BiFunction to be executed in the new thread
     * @param input1 the first input to be passed to the bi-function
     * @param input2 the second input to be passed to the bi-function
     * @return the started Thread object
     */
    public static <T, U, R> Thread newThread(BiFunction<T, U, R> biFunction, T input1, U input2) {
        return newThread(() -> biFunction.apply(input1, input2));
    }

    /**
     * Creates and starts a new thread with the specified name to execute the given BiFunction with the inputs.
     * The thread will be created as a non-daemon thread.
     * Note: The return value of the BiFunction is discarded.
     *
     * @param <T> the type of the first input to the bi-function
     * @param <U> the type of the second input to the bi-function
     * @param <R> the type of the result of the bi-function
     * @param biFunction the BiFunction to be executed in the new thread
     * @param input1 the first input to be passed to the bi-function
     * @param input2 the second input to be passed to the bi-function
     * @param name the name to assign to the new thread, or null for a default name
     * @return the started Thread object
     */
    public static <T, U, R> Thread newThread(BiFunction<T, U, R> biFunction, T input1, U input2, String name) {
        return newThread(() -> biFunction.apply(input1, input2), name);
    }

    /**
     * Creates and starts a new thread with the specified name and daemon status to execute the given BiFunction with the inputs.
     * Note: The return value of the BiFunction is discarded.
     *
     * @param <T> the type of the first input to the bi-function
     * @param <U> the type of the second input to the bi-function
     * @param <R> the type of the result of the bi-function
     * @param biFunction the BiFunction to be executed in the new thread
     * @param input1 the first input to be passed to the bi-function
     * @param input2 the second input to be passed to the bi-function
     * @param name the name to assign to the new thread, or null for a default name
     * @param daemon whether the thread should be a daemon thread
     * @return the started Thread object
     */
    public static <T, U, R> Thread newThread(BiFunction<T, U, R> biFunction, T input1, U input2, String name, boolean daemon) {
        return newThread(() -> biFunction.apply(input1, input2), name, daemon);
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
