package com.headstartech.burro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementing basic lifecycle behaviour for a {@link WorkQueueWorker}.
 */
public abstract class AbstractWorkQueueWorker implements WorkQueueWorker {

    private static final Logger logger = LoggerFactory.getLogger(AbstractWorkQueueWorker.class);

    private final Object haltedLock = new Object();
    private final Object finishedLock = new Object();
    private volatile boolean halted;

    protected boolean isHalted() {
        return halted;
    }

    protected abstract void doWork() throws InterruptedException;

    protected void doShutdownWork() {}

    @Override
    public void run() {
        try {
            while (!isHalted()) {
                doWork();
            }
            doShutdownWork();
        } catch (InterruptedException e) {
            logger.warn("Thread interrupted, finishing...");
        }

    }

    @Override
    public void shutdown() {
        synchronized (haltedLock) {
            halted = true;
            haltedLock.notifyAll();
        }
    }

    protected void waitUntilTimeoutOrHalted(long timeout) throws InterruptedException {
        synchronized (haltedLock) {
            haltedLock.wait(timeout);
        }
    }

}
