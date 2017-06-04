package com.headstartech.burro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Implementing basic lifecycle behaviour for a {@link WorkQueue}.
 */
public abstract class AbstractWorkQueue<T> implements WorkQueue<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractWorkQueue.class);

    private enum State { CREATED, STARTED, SHUTTING_DOWN, SHUTDOWN};

    private volatile State state;
    private final String queueName;

    AbstractWorkQueue(String queueName) {
        state = State.CREATED;
        this.queueName = queueName;
    }

    protected abstract boolean addToQueue(T item);

    protected abstract boolean addToQueue(T item, long duration, TimeUnit unit) throws InterruptedException;

    protected abstract int queueRemainingCapacity();

    protected abstract void onStarting();

    protected abstract void onShuttingDown();

    protected Logger getLogger() {
        return logger;
    }

    protected String getQueueName() {
        return queueName;
    }

    @Override
    public boolean add(T item) {
        validateAdd();
        return addToQueue(item);
    }

    @Override
    public boolean add(T item, long duration, TimeUnit unit) throws InterruptedException {
        validateAdd();
        return addToQueue(item, duration, unit);
    }

    @Override
    public int remainingCapacity() {
        return queueRemainingCapacity();
    }

    @Override
    public synchronized void start() {
        if(State.SHUTTING_DOWN.equals(state) || State.SHUTDOWN.equals(state)) {
            throw new IllegalStateException("The work queue cannot be started after shutdown() has been called.");
        }

        if(State.STARTED.equals(state)) {
            return;
        }

        onStarting();
        state = State.STARTED;
        getLogger().info("Work queue started: queueName={}", queueName);
    }

    @Override
    public synchronized void shutdown() {
        if(State.SHUTTING_DOWN.equals(state) || State.SHUTDOWN.equals(state)) {
            return;
        }

        state = State.SHUTTING_DOWN;
        getLogger().info("Shutting down work queue: queueName={}", queueName);
        onShuttingDown();
        state = State.SHUTDOWN;
        getLogger().info("Work queue shut down: queueName={}", queueName);
    }

    private void validateAdd() {
        if(State.SHUTTING_DOWN.equals(state)) {
            throw new IllegalStateException("The work queue is shutting down.");
        }
        if(State.SHUTDOWN.equals(state)) {
            throw new IllegalStateException("The work queue has been shutdown.");
        }
    }

}
