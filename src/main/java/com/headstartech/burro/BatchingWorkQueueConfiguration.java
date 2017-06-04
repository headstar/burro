package com.headstartech.burro;

/**
 * Configuration for a {@link BatchingWorkQueue}.
 */
public interface BatchingWorkQueueConfiguration {

    /**
     *  The maximum number of millliseconds to wait before writing.
     *
     * @return
     */
    int getMaxWriteDelay();

    /**
     *  The minimum number of millliseconds to wait before writing.
     *
     * @return
     */
    int getMinWriteDelay();

    /**
     * The minimum number of operations to include in each batch.
     *
     * @return
     */
    int getMinBatchSize();

    /**
     * The maximm number of operations to include in each batch.
     *
     * @return
     */
    int getMaxBatchSize();

    /**
     * Indicates if the contents of the queue should be written when shutting down.
     *
     * @return
     */
    boolean isWaitForCompletionOnShutdown();

    /**
     * The queue name.
     * @return
     */
    String getQueueName();

    /**
     * The number of milliseconds to wait when the queue is empty.
     *
     * @return
     */
    int getSleepIntervalWhenEmpty();

}
