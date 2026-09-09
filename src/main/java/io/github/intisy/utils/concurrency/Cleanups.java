package io.github.intisy.utils.concurrency;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Closers that must run when the JVM stops, however it stops.
 *
 * <p>Register a closer when you acquire something a dying JVM would otherwise leak, and unregister
 * it once you have released it yourself. {@link #installShutdownHook()} arms the single hook that
 * runs whatever is still registered.
 *
 * @author Finn Birich
 * @implNote a finally block covers an exception and nothing else. It does not run on Ctrl+C, and it
 * does not run on {@code System.exit}, which a command line tool reaches on every failure path. So
 * an interrupted run leaves its child processes alive and its remote resources allocated with
 * nobody reading the result. A hard kill runs nothing at all, which is what a startup sweep exists
 * for; this covers the two paths a process can still act on.
 * @implNote one shared registry with one hook, rather than a hook per library. Several independent
 * shutdown hooks in a JVM run in undefined order relative to each other, so a library that kills a
 * process another library is still talking to produces failures that depend on thread scheduling.
 */
@SuppressWarnings("unused")
public final class Cleanups {
    private static final long DEFAULT_BUDGET_MS = 10_000;
    private static final Cleanups SHARED = new Cleanups(DEFAULT_BUDGET_MS);

    private static boolean hookInstalled;

    /**
     * One thing to release. {@link AutoCloseable} rather than a type of our own, because the shape
     * is identical and callers already have methods that fit it.
     */
    public interface Closer extends AutoCloseable {
        @Override
        void close() throws Exception;
    }

    /**
     * A registered closer, and the handle used to unregister it again.
     */
    public static final class Registration {
        private final String what;
        private final Closer closer;

        Registration(String what, Closer closer) {
            this.what = what;
            this.closer = closer;
        }

        public String what() {
            return what;
        }

        public Closer closer() {
            return closer;
        }
    }

    private final Deque<Registration> pending = new ArrayDeque<Registration>();
    private final long budgetMs;

    public Cleanups(long budgetMs) {
        this.budgetMs = budgetMs;
    }

    public static Cleanups shared() {
        return SHARED;
    }

    public static synchronized void installShutdownHook() {
        if (hookInstalled) {
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                SHARED.runAll();
            }
        }, "cleanups"));
        hookInstalled = true;
    }

    public synchronized Registration register(String what, Closer closer) {
        Registration registration = new Registration(what, closer);
        pending.push(registration);
        return registration;
    }

    /**
     * @implNote every caller unregisters after cleaning up itself, so a long run does not accumulate
     * closers for work that is already finished and the hook does not repeat it.
     */
    public synchronized void unregister(Registration registration) {
        pending.remove(registration);
    }

    public synchronized int pendingCount() {
        return pending.size();
    }

    /**
     * @implNote reverse order of registration, guarded per closer and bounded overall. A shutdown
     * hook that hangs prevents the JVM from exiting at all, so exhausting the budget names what was
     * skipped rather than waiting: an operator who has to finish by hand needs to be told which
     * things.
     */
    public void runAll() {
        long deadline = System.currentTimeMillis() + budgetMs;
        for (Registration registration = next(); registration != null; registration = next()) {
            if (System.currentTimeMillis() >= deadline) {
                reportSkipped(registration);
                return;
            }
            try {
                registration.closer().close();
            } catch (Exception failure) {
                System.err.println("cleanup '" + registration.what() + "' failed: " + failure);
            }
        }
    }

    private synchronized Registration next() {
        return pending.poll();
    }

    private void reportSkipped(Registration current) {
        List<String> skipped = new ArrayList<String>();
        skipped.add(current.what());
        for (Registration registration = next(); registration != null; registration = next()) {
            skipped.add(registration.what());
        }
        System.err.println("cleanup budget of " + budgetMs + "ms exhausted, NOT cleaned up: " + skipped);
    }
}
