package io.github.intisy.utils.utils;

@SuppressWarnings({"unused", "InfiniteLoopStatement"})
public class ThreadUtils {
    public static void sleep() {
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

    public static void newThread(Runnable runnable) {
        new Thread(runnable).start();
    }
    public static void newThread(Runnable runnable, String name) {
        new Thread(runnable, name).start();
    }
    public static void newThread(Runnable runnable, String name, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.start();
    }

    public static String getThreadName() {
        return Thread.currentThread().getName();
    }

    public static void printThreadName() {
        System.out.println("Current thread: " + ThreadUtils.getThreadName());
    }
}
