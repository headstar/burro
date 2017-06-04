package com.headstartech.burro;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchingWorkQueueTest {

    @Test
    public void testMaxDelay() throws InterruptedException {
        BatchingWorkQueue<WorkQueueItem> workQueue = null;
        try {
            // given
            int maxDelay = 200;
            BatchingWorkQueueConfiguration configuration = new MutableBatchingWorkQueueConfiguration("test-queue",
                    10,        // min batch
                    10,        // max batch
                    10, maxDelay,  // max delay
                    // min delay
                    true,
                    10);

            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            WorkProcessorStub<WorkQueueItem> workProcessorStub = new WorkProcessorStub<WorkQueueItem>(1);
            workQueue = new BatchingWorkQueue<WorkQueueItem>(configuration, queue, workProcessorStub);

            WorkQueueItem a = new WorkQueueItem();
            WorkQueueItem b = new WorkQueueItem();

            workQueue.start();
            // when
            Date timeAdded = new Date();
            workQueue.add(a);
            workQueue.add(b);

            // then
            workProcessorStub.processedLatch.await(10000, TimeUnit.MILLISECONDS);
            assertEquals(1, workProcessorStub.processedWork.size());
            Pair<Date, List<WorkQueueItem>> pair = workProcessorStub.processedWork.get(0);
            long delay = pair.first.getTime() - timeAdded.getTime();
            assertTrue(delay >= (maxDelay - 5));  // some margin
            assertEquals(2, pair.second.size());
            assertEquals(a, pair.second.get(0));
            assertEquals(b, pair.second.get(1));
        } finally {
            if(workQueue != null) {
                workQueue.shutdown();
            }
        }
    }

    @Test
    public void testMinDelay() throws InterruptedException {
        BatchingWorkQueue<WorkQueueItem> workQueue = null;
        try {
            int minDelay = 200;
            // given
            BatchingWorkQueueConfiguration configuration = new MutableBatchingWorkQueueConfiguration("test-queue",
                    1,   // min batch
                    1,   // max batch
                    minDelay, 1000,
                    // min delay
                    true,
                    10);

            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            WorkProcessorStub<WorkQueueItem> workProcessorStub = new WorkProcessorStub<WorkQueueItem>(2);
            workQueue = new BatchingWorkQueue<WorkQueueItem>(configuration, queue, workProcessorStub);

            WorkQueueItem a = new WorkQueueItem();
            WorkQueueItem b = new WorkQueueItem();
            workQueue.add(a);
            workQueue.add(b);

            // when
            workQueue.start();

            // then
            workProcessorStub.processedLatch.await(10000, TimeUnit.MILLISECONDS);
            assertEquals(2, workProcessorStub.processedWork.size());
            Pair<Date, List<WorkQueueItem>> firstPair = workProcessorStub.processedWork.get(0);
            assertEquals(1, firstPair.second.size());
            assertEquals(a, firstPair.second.get(0));

            Pair<Date, List<WorkQueueItem>> secondPair = workProcessorStub.processedWork.get(1);
            assertEquals(1, secondPair.second.size());
            assertEquals(b, secondPair.second.get(0));

            assertTrue(secondPair.first.getTime() - firstPair.first.getTime() >= minDelay);
        } finally {
            if(workQueue != null) {
                workQueue.shutdown();
            }
        }
    }

    @Test
    public void testMinBatch() throws InterruptedException {
        BatchingWorkQueue<WorkQueueItem> workQueue = null;
        try {
            // given
            BatchingWorkQueueConfiguration configuration = new MutableBatchingWorkQueueConfiguration("test-queue",
                    2,  // min batch
                    10,
                    10, 1000,
                    true, 10);

            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            WorkProcessorStub<WorkQueueItem> workProcessorStub = new WorkProcessorStub<WorkQueueItem>(1);
            workQueue = new BatchingWorkQueue<WorkQueueItem>(configuration, queue, workProcessorStub);

            WorkQueueItem a = new WorkQueueItem();
            WorkQueueItem b = new WorkQueueItem();
            workQueue.add(a);
            workQueue.add(b);

            // when
            workQueue.start();
            Date timeAdded = new Date();

            // then
            workProcessorStub.processedLatch.await(10000, TimeUnit.MILLISECONDS);
            assertEquals(1, workProcessorStub.processedWork.size());
            Pair<Date, List<WorkQueueItem>> pair = workProcessorStub.processedWork.get(0);
            assertTrue(pair.first.getTime() - timeAdded.getTime() <= 50);  // should be processed immediately
            assertEquals(2, pair.second.size());
            assertEquals(a, pair.second.get(0));
            assertEquals(b, pair.second.get(1));
        } finally {
            if(workQueue != null) {
                workQueue.shutdown();
            }
        }
    }

    @Test
    public void testMaxBatch() throws InterruptedException {
        BatchingWorkQueue<WorkQueueItem> workQueue = null;
        try {
            // given
            BatchingWorkQueueConfiguration configuration = new MutableBatchingWorkQueueConfiguration("test-queue",
                    2,  // min batch
                    2,  // max batch
                    10, 100,
                    true, 10);

            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            WorkProcessorStub<WorkQueueItem> workProcessorStub = new WorkProcessorStub<WorkQueueItem>(2);
            workQueue = new BatchingWorkQueue<WorkQueueItem>(configuration, queue, workProcessorStub);

            WorkQueueItem a = new WorkQueueItem();
            WorkQueueItem b = new WorkQueueItem();
            WorkQueueItem c = new WorkQueueItem();
            WorkQueueItem d = new WorkQueueItem();
            workQueue.add(a);
            workQueue.add(b);
            workQueue.add(c);
            workQueue.add(d);

            // when
            workQueue.start();

            // then
            workProcessorStub.processedLatch.await(10000, TimeUnit.MILLISECONDS);
            assertEquals(2, workProcessorStub.processedWork.size());
            Pair<Date, List<WorkQueueItem>> firstPair = workProcessorStub.processedWork.get(0);
            assertEquals(2, firstPair.second.size());
            assertEquals(a, firstPair.second.get(0));
            assertEquals(b, firstPair.second.get(1));

            Pair<Date, List<WorkQueueItem>> secondPair = workProcessorStub.processedWork.get(1);
            assertEquals(2, secondPair.second.size());
            assertEquals(c, secondPair.second.get(0));
            assertEquals(d, secondPair.second.get(1));
        } finally {
            if(workQueue != null) {
                workQueue.shutdown();
            }
        }
    }

    @Test
    public void testShutdownWithItemsInQueue() throws InterruptedException {
        BatchingWorkQueue<WorkQueueItem> workQueue = null;
        try {
            // given
            BatchingWorkQueueConfiguration configuration = new MutableBatchingWorkQueueConfiguration("test-queue",
                    2,  // min batch
                    2,  // max batch
                    10, 100,
                    true, 10);

            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            WorkProcessorStub<WorkQueueItem> workProcessorStub = new WorkProcessorStub<WorkQueueItem>(3);
            workQueue = new BatchingWorkQueue<WorkQueueItem>(configuration, queue, workProcessorStub);

            WorkQueueItem a = new WorkQueueItem();
            WorkQueueItem b = new WorkQueueItem();
            WorkQueueItem c = new WorkQueueItem();
            WorkQueueItem d = new WorkQueueItem();
            WorkQueueItem e = new WorkQueueItem();
            WorkQueueItem f = new WorkQueueItem();
            workQueue.add(a);
            workQueue.add(b);
            workQueue.add(c);
            workQueue.add(d);
            workQueue.add(e);
            workQueue.add(f);

            workQueue.start();

            // when
            workQueue.shutdown();

            // then
            workProcessorStub.processedLatch.await(10000, TimeUnit.MILLISECONDS);
            assertEquals(3, workProcessorStub.processedWork.size());
            Pair<Date, List<WorkQueueItem>> firstPair = workProcessorStub.processedWork.get(0);
            assertEquals(2, firstPair.second.size());
            assertEquals(a, firstPair.second.get(0));
            assertEquals(b, firstPair.second.get(1));

            Pair<Date, List<WorkQueueItem>> secondPair = workProcessorStub.processedWork.get(1);
            assertEquals(2, secondPair.second.size());
            assertEquals(c, secondPair.second.get(0));
            assertEquals(d, secondPair.second.get(1));

            Pair<Date, List<WorkQueueItem>> thirdPair = workProcessorStub.processedWork.get(2);
            assertEquals(2, thirdPair.second.size());
            assertEquals(e, thirdPair.second.get(0));
            assertEquals(f, thirdPair.second.get(1));
        } finally {
            if(workQueue != null) {
                workQueue.shutdown();
            }
        }
    }

    private static class WorkProcessorStub<T> implements WorkProcessor<Collection<T>> {

        private static final Logger logger = LoggerFactory.getLogger(WorkProcessorStub.class);
        private final CountDownLatch processedLatch;
        private final List<Pair<Date, List<T>>> processedWork = new ArrayList<Pair<Date, List<T>>>();

        WorkProcessorStub(int n) {
            processedLatch = new CountDownLatch(n);
        }

        @Override
        public void process(Collection<T> work) {
            logger.info("Processing work");
            processedWork.add(new Pair<Date, List<T>>(new Date(), new ArrayList<T>(work)));
            processedLatch.countDown();
        }
    }

    private static class Pair<T, U> {

        private final T first;
        private final U second;

        public Pair(T first, U second) {
            this.first = first;
            this.second = second;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }
    }

}
