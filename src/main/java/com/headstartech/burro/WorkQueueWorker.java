package com.headstartech.burro;

/**
 * Interface for WorkQueue implementations using a background worker thread.
 */
public interface WorkQueueWorker extends Runnable {

    void shutdown();
}
