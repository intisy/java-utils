package io.github.intisy.utils.concurrency;

/**
 * Utility class providing functionality for retrying operations that might fail.
 * This class allows operations to be retried indefinitely until they succeed,
 * with optional wait periods between retry attempts.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class RetryUtils {
    /**
     * Executes the provided operation and retries indefinitely until it succeeds.
     * If the operation fails, it will be retried immediately without any delay.
     *
     * @param retryable the operation to execute and potentially retry
     */
    public static void run(Retryable retryable) {
        run(0, retryable);
    }

    /**
     * Executes the provided operation and retries indefinitely until it succeeds.
     * If the operation fails, it will be retried after waiting for the specified
     * number of milliseconds. If wait is 0 or negative, retries will happen immediately.
     *
     * @param wait the number of milliseconds to wait between retry attempts
     * @param retryable the operation to execute and potentially retry
     */
    public static void run(int wait, Retryable retryable) {
        while (true) {
            try {
                retryable.run();
                break;
            } catch (Exception e) {
                if (wait > 0)
                    ThreadUtils.sleep(wait);
            }
        }
    }

    /**
     * Interface for operations that can be retried.
     * Implementations of this interface represent operations that might fail
     * and need to be retried until they succeed.
     */
    public interface Retryable {
        /**
         * Executes the retryable operation.
         *
         * @throws Exception if the operation fails and should be retried
         */
        void run() throws Exception;
    }
}
