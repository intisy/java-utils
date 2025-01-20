package io.github.intisy.utils.custom;

import io.github.intisy.simple.logger.Log;
import io.github.intisy.utils.utils.ThreadUtils;

public class Retry {
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
                ThreadUtils.sleep(wait);
            }
        }
    }

    public interface Retryable {
        void run() throws Exception;
    }
}
