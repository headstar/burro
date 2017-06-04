package com.headstartech.burro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * {@link WorkQueue} processing the items in batches.
 */
public class BatchingWorkQueue<T> extends AbstractWorkQueue<T> {

    private final Logger logger = LoggerFactory.getLogger(BatchingWorkQueue.class);

    private final boolean shutdownWait;
    private Thread queueWorkerThread;
    private String queueWorkerThreadName;

    private final BlockingQueue<T> queue;
    private final BatchingQueueWorker<T> worker;

    public BatchingWorkQueue(BatchingWorkQueueConfiguration configuration, BlockingQueue<T> queue, WorkProcessor<Collection<T>> processor) {
        super(configuration.getQueueName());
        this.queue = queue;
        this.shutdownWait = configuration.isWaitForCompletionOnShutdown();
        worker = new BatchingQueueWorker<T>(configuration, processor, queue);

    }

    @Override
    protected boolean addToQueue(T item) {
        return queue.offer(item);
    }

    @Override
    protected boolean addToQueue(T item, long duration, TimeUnit unit) throws InterruptedException {
        return queue.offer(item, duration, unit);
    }

    @Override
    protected int queueRemainingCapacity() {
        return queue.remainingCapacity();
    }

    @Override
    protected void onStarting() {
        queueWorkerThreadName = String.format("%s-worker", getQueueName());
        logger.info("Starting worker thread: queueName={}, queueWorkerThreadName={}",  getQueueName(), queueWorkerThreadName);
        queueWorkerThread = new Thread(worker, queueWorkerThreadName);
        queueWorkerThread.setDaemon(true);
        queueWorkerThread.start();
    }

    @Override
    protected void onShuttingDown() {
        try {
            worker.shutdown();
        } catch(RuntimeException e) {
            logger.warn("Exception caught when shutting down: queueName={}", getQueueName(), e);
        }
        if(shutdownWait) {
            boolean interrupted = false;
            try {
                while (queueWorkerThread.isAlive()) {
                    try {
                        logger.debug("Waiting for queue worker thread to finish: queueName={}, queueWorkerThreadName={}", getQueueName(), queueWorkerThreadName);
                        queueWorkerThread.join(1000);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                logger.debug("Queue worker thread finished: queueName={}, queueWorkerThreadName={}", getQueueName(), queueWorkerThreadName);
            } finally {
                if(interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
