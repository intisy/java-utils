package io.github.intisy.utils.custom;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("unused")
public class Wait {
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private boolean variable = false;

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

    public void setVariable(boolean value) {
        lock.lock();
        try {
            variable = value;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
    public boolean getVariable() {
        return variable;
    }
}