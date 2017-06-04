package com.headstartech.burro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Worker for a {@link BatchingWorkQueue}.
 * Waiting for enough items in the queue or enough time has passed before processing the items in batches.
 */
class BatchingQueueWorker<T> extends AbstractWorkQueueWorker {

    private static final Logger logger = LoggerFactory.getLogger(BatchingQueueWorker.class);

    private final BatchingWorkQueueConfiguration configuration;
    private final WorkProcessor<Collection<T>> processor;
    private long lastProcessing = System.currentTimeMillis();
    private long lastWorkDone = System.currentTimeMillis();
    private final BlockingQueue<T> queue;
    private final String queueName;


    public BatchingQueueWorker(BatchingWorkQueueConfiguration configuration, WorkProcessor<Collection<T>> processor, BlockingQueue<T> queue) {
        this.configuration = configuration;
        this.processor = processor;
        this.queue = queue;
        this.queueName = configuration.getQueueName();
    }

    @Override
    protected void doWork() throws InterruptedException {
        processItems(false);
        waitForMoreWork();
    }

    @Override
    protected void doShutdownWork() {
        logger.debug("Starting shutdown work...");
;       processItems(true);
        logger.debug("Finished shutdown work");
    }

    private void waitForMoreWork() throws InterruptedException {
        int minDelay = configuration.getMinWriteDelay();
        if (minDelay > 0) {
            logger.debug("Sleeping...: queueName={}, minDelay={}", queueName, minDelay);
            waitUntilTimeoutOrHalted(minDelay);
        }
        if (configuration.getSleepIntervalWhenEmpty() > 0) {
            while (!isHalted() && getQueue().isEmpty()) {
                int sleepIntervalWhenEmpty = configuration.getSleepIntervalWhenEmpty();
                logger.debug("Queue empty, sleeping...: queueName={}, sleepIntervalWhenEmpty={}", queueName, sleepIntervalWhenEmpty);
                waitUntilTimeoutOrHalted(sleepIntervalWhenEmpty);
            }
        }
    }

    private void processItems(boolean forceDoWorkOnShutdown) {
        lastProcessing = System.currentTimeMillis();
        if (!forceDoWorkOnShutdown) {
            int queueSize = getQueue().size();
            if(queueSize >= configuration.getMinBatchSize()) {
                logger.debug("Queue size greater than or equals to minBatchSize: queueName={}, queueSize={}, minBatchSize={}", queueName, queueSize, configuration.getMinBatchSize());
            } else if(configuration.getMaxWriteDelay() < (lastProcessing - lastWorkDone)) {
                logger.debug("More than maxDelay ms has passed since last time items were processed: queueName={}, maxDelay={}, timePassed={}", queueName, configuration.getMaxWriteDelay(), lastProcessing - lastWorkDone);
            } else {
                return;
            }
        }

        lastWorkDone = System.currentTimeMillis();

        if(getQueue().isEmpty()) {
            return;
        }

        do {
            List<T> itemsToProcess = new LinkedList<T>();
            getQueue().drainTo(itemsToProcess, configuration.getMaxBatchSize());
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing items...: queueName={}, itemCount={}, queueSize={}, maxBatchSize={}", queueName, itemsToProcess.size(), getQueue().size(), configuration.getMaxBatchSize());
                }
                processor.process(itemsToProcess);
            } catch (Throwable t) {
                logger.warn(String.format("Exception caught when processing items: queueName=%s", queueName), t);
            }
        } while(forceDoWorkOnShutdown && !getQueue().isEmpty());
    }

    private BlockingQueue<T> getQueue() {
        return queue;
    }
}
