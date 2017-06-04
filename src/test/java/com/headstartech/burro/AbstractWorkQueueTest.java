package com.headstartech.burro;


import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AbstractWorkQueueTest {

    @Test(expected = IllegalStateException.class)
    public void testStartWhenShutdown() throws InterruptedException {
        // given
        WorkQueueWorkerStub workerStub = null;
        try {
            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            workerStub = new WorkQueueWorkerStub();
            WorkQueue<WorkQueueItem> workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);
            workQueue.start();
            workQueue.shutdown();

            // when
            workQueue.start();

            // then ...exception should be thrown
        } finally {
            if(workerStub != null) {
                workerStub.shutdown();
            }
        }
    }

    @Test
    public void testStart() throws InterruptedException {
        // given
        WorkQueueWorkerStub workerStub = null;
        try {
            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            workerStub = new WorkQueueWorkerStub();
            WorkQueue<WorkQueueItem> workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);

            // when
            workQueue.start();

            // then
            Thread.sleep(100);
            assertTrue(workerStub.isRunning());
            assertFalse(workerStub.isShutdown());
        } finally {
            if(workerStub != null) {
                workerStub.shutdown();
            }
        }
    }

    @Test
    public void testStartWhenStarted() throws InterruptedException {
        // given
        WorkQueueWorkerStub workerStub = null;
        try {
            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            workerStub = new WorkQueueWorkerStub();
            WorkQueue<WorkQueueItem> workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);
            workQueue.start();
            Thread.sleep(100);
            assertTrue(workerStub.isRunning());
            assertFalse(workerStub.isShutdown());

            // when
            workQueue.start();

            // then ...no exception should haven been thrown
        } finally {
            if(workerStub != null) {
                workerStub.shutdown();
            }
        }
    }

    @Test
    public void testShutdown() throws InterruptedException {
        // given
        WorkQueueWorkerStub workerStub = null;
        try {
            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            workerStub = new WorkQueueWorkerStub();
            WorkQueue<WorkQueueItem> workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);
            workQueue.start();
            Thread.sleep(100);
            assertTrue(workerStub.isRunning());

            // when
            workQueue.shutdown();

            // then
            Thread.sleep(100);
            assertFalse(workerStub.isRunning());
            assertTrue(workerStub.isShutdown());
        } finally {
            if(workerStub != null) {
                workerStub.shutdown();
            }
        }
    }

    @Test
    public void testAddWhenStarted() throws InterruptedException {
        // given
        WorkQueue<WorkQueueItem> workQueue = null;
        try {
            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            WorkQueueWorkerStub workerStub = new WorkQueueWorkerStub();
            workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);
            workQueue.start();
            Thread.sleep(100);
            assertTrue(workerStub.isRunning());

            WorkQueueItem item = new WorkQueueItem();

            // when
            boolean res = workQueue.add(item);

            // then
            assertTrue(res);
            assertTrue(queue.contains(item));

        } finally {
            if(workQueue != null) {
                workQueue.shutdown();
            }
        }
    }

    @Test
    public void testAddWhenNotStarted() throws InterruptedException {
        // given
        WorkQueue<WorkQueueItem> workQueue = null;
        BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
        WorkQueueWorkerStub workerStub = new WorkQueueWorkerStub();
        workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);

        WorkQueueItem item = new WorkQueueItem();

        // when
        boolean res = workQueue.add(item);

        // then
        assertTrue(res);
        assertTrue(queue.contains(item));
    }

    @Test(expected = IllegalStateException.class)
    public void testAddWhenShutdown() throws InterruptedException {
        // given
        WorkQueue<WorkQueueItem> workQueue = null;
        BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
        WorkQueueWorkerStub workerStub = new WorkQueueWorkerStub();
        workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);
        workQueue.start();
        workQueue.shutdown();

        WorkQueueItem item = new WorkQueueItem();

        // when
        boolean res = workQueue.add(item);

        // then...exception should be thrown
    }

    @Test
    public void testRemainingCapacity() throws InterruptedException {
        // given
        WorkQueue<WorkQueueItem> workQueue = null;
        BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(17);
        WorkQueueWorkerStub workerStub = new WorkQueueWorkerStub();
        workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);

        // when
        int remainingCapacity = workQueue.remainingCapacity();

        // then
        assertEquals(17, remainingCapacity);
    }

    @Test
    public void testAddWithWait() throws InterruptedException {
        // given
        WorkQueue<WorkQueueItem> workQueue = null;
        try {
            BlockingQueue<WorkQueueItem> queue = new ArrayBlockingQueue<WorkQueueItem>(100);
            WorkQueueWorkerStub workerStub = new WorkQueueWorkerStub();
            workQueue = new WorkQueueStub<WorkQueueItem>("test", queue, workerStub);
            workQueue.start();
            Thread.sleep(100);
            assertTrue(workerStub.isRunning());

            WorkQueueItem item = new WorkQueueItem();

            // when
            boolean res = workQueue.add(item, 1000, TimeUnit.MILLISECONDS);

            // then
            assertTrue(res);
            assertTrue(queue.contains(item));

        } finally {
            if(workQueue != null) {
                workQueue.shutdown();
            }
        }
    }

    private static class WorkQueueStub<T> extends AbstractWorkQueue<T> {

        private final BlockingQueue<T> queue;
        private final WorkQueueWorker workQueueWorker;
        private Thread workerThread;

        public WorkQueueStub(String queueName, BlockingQueue<T> queue, WorkQueueWorker workQueueWorker) {
            super(queueName);
            this.queue = queue;
            this.workQueueWorker = workQueueWorker;
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
            workerThread = new Thread(workQueueWorker);
            workerThread.start();
        }

        @Override
        protected void onShuttingDown() {
            try {
                if(workerThread.isAlive()) {
                    workQueueWorker.shutdown();
                    workerThread.join();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class WorkQueueWorkerStub implements WorkQueueWorker {

        private static final Logger logger = LoggerFactory.getLogger(WorkQueueWorkerStub.class);

        private volatile boolean shutdown = false;
        private volatile boolean running = false;

        @Override
        public void run() {
            logger.info("Running...");
            running = true;
            while(!shutdown) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            running = false;
        }

        @Override
        public void shutdown() {
            logger.info("Shutdown...");
            shutdown = true;
        }

        public boolean isShutdown() {
            return shutdown;
        }

        public boolean isRunning() {
            return running;
        }
    }

}
