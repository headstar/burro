package com.headstartech.burro;

/**
 * Configuration for a {@link BatchingWorkQueue}.
 */
public interface BatchingWorkQueueConfiguration {

    /**
     *  The maximum number of millliseconds to wait before writing.
     *
     * @return the max write delay
     */
    int getMaxWriteDelay();

    /**
     *  The minimum number of millliseconds to wait before writing.
     *
     * @return minimum number of millliseconds to wait before writing
     */
    int getMinWriteDelay();

    /**
     * The minimum number of operations to include in each batch.
     *
     * @return minimum number of operations to include in each batch
     */
    int getMinBatchSize();

    /**
     * The maximm number of operations to include in each batch.
     *
     * @return maximm number of operations to include in each batch
     */
    int getMaxBatchSize();

    /**
     * Indicates if the contents of the queue should be written when shutting down.
     *
     * @return {@code true} if the contents of the queue will be written when shutting down, {@code false} otherwise
     */
    boolean isWaitForCompletionOnShutdown();

    /**
     * The queue name.
     * @return the name of the queue
     */
    String getQueueName();

    /**
     * The number of milliseconds to wait when the queue is empty.
     *
     * @return number of milliseconds to wait when the queue is empty
     */
    int getSleepIntervalWhenEmpty();

}
