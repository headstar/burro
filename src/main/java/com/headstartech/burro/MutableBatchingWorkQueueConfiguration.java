package com.headstartech.burro;


/**
 * Mutable (and thread-safe) implementation of {@link BatchingWorkQueueConfiguration}.
 */
public class MutableBatchingWorkQueueConfiguration implements BatchingWorkQueueConfiguration {

    private final String queueName;
    private volatile int minBatchSize;
    private volatile int maxBatchSize;
    private volatile int maxWriteDelay;
    private volatile int minWriteDelay;
    private volatile boolean waitForCompletionOnShutdown;
    private volatile int sleepIntervalWhenEmpty;

    public MutableBatchingWorkQueueConfiguration(String queueName, int minBatchSize, int maxBatchSize, int minWriteDelay, int maxWriteDelay, boolean waitForCompletionOnShutdown, int sleepIntervalWhenEmpty) {
        this.queueName = queueName;
        setMaxBatchSize(maxBatchSize);
        setMinBatchSize(minBatchSize);
        setMaxWriteDelay(maxWriteDelay);
        setMinWriteDelay(minWriteDelay);
        setWaitForCompletionOnShutdown(waitForCompletionOnShutdown);
        setSleepIntervalWhenEmpty(sleepIntervalWhenEmpty);
    }

    public int getMaxWriteDelay() {
        return maxWriteDelay;
    }

    public int getMinWriteDelay() {
        return minWriteDelay;
    }

    public int getMinBatchSize() {
        return minBatchSize;
    }

    public boolean isWaitForCompletionOnShutdown() {
        return waitForCompletionOnShutdown;
    }

    public String getQueueName() {
        return queueName;
    }

    public int getSleepIntervalWhenEmpty() {
        return sleepIntervalWhenEmpty;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMinBatchSize(int minBatchSize) {
        checkArgument(minBatchSize >=1, "minBatchSize must be >= 1");
        checkArgument(minBatchSize <= maxBatchSize, "minBatchSize must be <= maxBatchSize");
        this.minBatchSize = minBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        checkArgument(maxBatchSize >=1, "maxBatchSize must be >= 1");
        checkArgument(maxBatchSize >= minBatchSize, "maxBatchSize must be >= minBatchSize");
        this.maxBatchSize = maxBatchSize;
    }

    public void setMaxWriteDelay(int maxWriteDelay) {
        checkArgument(maxWriteDelay >= 0, "maxDelay must be >= 0");
        checkArgument(maxWriteDelay >= minWriteDelay, "maxDelay must be >= minDelay");
        this.maxWriteDelay = maxWriteDelay;
    }

    public void setMinWriteDelay(int minWriteDelay) {
        checkArgument(minWriteDelay >= 0, "minDelay must be >= 0");
        checkArgument(minWriteDelay <= maxWriteDelay, "minDelay must be <= maxDelay");
        this.minWriteDelay = minWriteDelay;
    }

    public void setWaitForCompletionOnShutdown(boolean waitForCompletionOnShutdown) {
        this.waitForCompletionOnShutdown = waitForCompletionOnShutdown;
    }

    public void setSleepIntervalWhenEmpty(int sleepIntervalWhenEmpty) {
        checkArgument(sleepIntervalWhenEmpty >= 0, "sleepIntervalWhenEmpty must be >= 0");
        this.sleepIntervalWhenEmpty = sleepIntervalWhenEmpty;
    }

    private static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }
}
