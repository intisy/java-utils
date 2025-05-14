package io.github.intisy.utils.utils;

import io.github.intisy.simple.logger.Log;

public class RetryUtils {
    public static void run(Retryable retryable) {
        run(0, retryable);
    }
    public static void run(int wait, Retryable retryable) {
        while (true) {
            try {
                retryable.run();
                break;
            } catch (Exception e) {
                Log.error(e);
                if (wait > 0)
                    ThreadUtils.sleep(wait);
            }
        }
    }

    public interface Retryable {
        void run() throws Exception;
    }
}
