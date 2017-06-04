package com.headstartech.burro;

import java.util.concurrent.TimeUnit;

/**
 * Main interface for the work queue.
 */
public interface WorkQueue<T> {

    /**
     * Adds an item to the queue. Will return immediately (return value indicates success/failure).
     *
     * @param item item to add
     * @return {@code true} if item was added, {@code false} otherwise
     */
    boolean add(T item);

    /**
     * Adds an item to the queue.
     *
     * @param item item to add
     * @param duration duration to wait
     * @param unit unit for duration
     * @return {@code true} if item was added, {@code false} otherwise
     * @throws InterruptedException if interrupted while waiting
     */
    boolean add(T item, long duration, TimeUnit unit) throws InterruptedException;

    /**
     * Returns the remaining capacity. <code>Integer.MAX_VALUE</code> if unbounded.
     *
     * @return the remaining capacity of the queue
     */
    int remainingCapacity();

    /**
     * Starts the queue worker thread. It's ok to add items before the queue is started.
     */
    void start();

    /**
     * Stops the queue worker thread. It's not possible to add items after shutdown.
     *
     * The work queue cannot be re-started.
     */
    void shutdown();

}
