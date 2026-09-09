package io.github.intisy.utils.concurrency;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CleanupsTest {

    /**
     * Reverse order is not decoration: a closer registered later can depend on something an earlier
     * one releases, so running forwards would tear the dependency down first.
     */
    @Test
    void runsClosersInReverseOrderOfRegistration() {
        Cleanups cleanups = new Cleanups(10_000);
        List<String> order = new ArrayList<String>();

        cleanups.register("first", record(order, "first"));
        cleanups.register("second", record(order, "second"));
        cleanups.register("third", record(order, "third"));
        cleanups.runAll();

        assertEquals(Arrays.asList("third", "second", "first"), order);
    }

    @Test
    void unregisteredCloserDoesNotRun() {
        Cleanups cleanups = new Cleanups(10_000);
        List<String> order = new ArrayList<String>();

        Cleanups.Registration kept = cleanups.register("kept", record(order, "kept"));
        Cleanups.Registration dropped = cleanups.register("dropped", record(order, "dropped"));
        cleanups.unregister(dropped);
        cleanups.runAll();

        assertEquals(Collections.singletonList("kept"), order);
        assertEquals(0, cleanups.pendingCount());
        assertEquals("kept", kept.what());
    }

    /**
     * A closer that throws must not strand the ones registered before it, which is why each is
     * guarded individually rather than the loop being wrapped once.
     */
    @Test
    void aFailingCloserDoesNotStopTheRest() {
        Cleanups cleanups = new Cleanups(10_000);
        List<String> order = new ArrayList<String>();

        cleanups.register("early", record(order, "early"));
        cleanups.register("boom", new Cleanups.Closer() {
            @Override
            public void close() {
                throw new IllegalStateException("closer failed");
            }
        });
        String errors = captureStderr(cleanups);

        assertEquals(Collections.singletonList("early"), order);
        assertTrue(errors.contains("boom"), "expected the failure to name the closer, got: " + errors);
    }

    /**
     * A shutdown hook that hangs stops the JVM exiting at all, so an exhausted budget must abandon
     * the remaining closers and say which ones it abandoned.
     */
    @Test
    void anExhaustedBudgetAbandonsTheRestAndNamesThem() {
        Cleanups cleanups = new Cleanups(0);
        List<String> order = new ArrayList<String>();

        cleanups.register("never-runs", record(order, "never-runs"));
        String errors = captureStderr(cleanups);

        assertEquals(Collections.<String>emptyList(), order);
        assertTrue(errors.contains("budget"), "expected the budget message, got: " + errors);
        assertTrue(errors.contains("never-runs"), "expected the skipped closer named, got: " + errors);
        assertEquals(0, cleanups.pendingCount(), "an abandoned run must still drain the registry");
    }

    @Test
    void pendingCountTracksRegistrationAndRun() {
        Cleanups cleanups = new Cleanups(10_000);
        assertEquals(0, cleanups.pendingCount());

        cleanups.register("one", record(new ArrayList<String>(), "one"));
        cleanups.register("two", record(new ArrayList<String>(), "two"));
        assertEquals(2, cleanups.pendingCount());

        cleanups.runAll();
        assertEquals(0, cleanups.pendingCount());
    }

    @Test
    void sharedIsOneInstanceForTheWholeJvm() {
        assertSame(Cleanups.shared(), Cleanups.shared());
    }

    private static Cleanups.Closer record(final List<String> order, final String name) {
        return new Cleanups.Closer() {
            @Override
            public void close() {
                order.add(name);
            }
        };
    }

    private static String captureStderr(Cleanups cleanups) {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(captured, true));
            cleanups.runAll();
        } finally {
            System.setErr(original);
        }
        return new String(captured.toByteArray(), Charset.forName("UTF-8"));
    }
}
