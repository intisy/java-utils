package io.github.intisy.utils.utils;

@SuppressWarnings({"unused", "InfiniteLoopStatement"})
public class ThreadUtils {
    public static void done() {
        while (true)
            ThreadUtils.sleep(1000);
    }
    public static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
