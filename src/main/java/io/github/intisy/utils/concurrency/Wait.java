package io.github.intisy.utils.concurrency;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A utility class for thread synchronization based on a boolean condition.
 * This class provides a mechanism for threads to wait until a boolean variable
 * reaches a specific value, using a lock and condition variable for thread safety.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class Wait {
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private boolean variable = false;

    /**
     * Causes the current thread to wait until the internal boolean variable
     * equals the specified value. This method will block until the condition is met
     * or the thread is interrupted.
     *
     * @param variable the boolean value to wait for
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void waitForVariable(boolean variable) throws InterruptedException {
        lock.lock();
        try {
            while (this.variable != variable) {
                condition.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets the internal boolean variable to the specified value and
     * notifies all waiting threads that the variable has changed.
     *
     * @param value the new value to set
     */
    public void setVariable(boolean value) {
        lock.lock();
        try {
            variable = value;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current value of the internal boolean variable.
     *
     * @return the current value of the variable
     */
    public boolean getVariable() {
        return variable;
    }
}