package com.jake.threadpool;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO ::
 * - Introduce builder API to introduce more customizable properties
 * - Make `queue` customizable
 * - Add `RejectedTaskHandler`
 * - Add 'UncaughtExceptionHandler`
 * - Implement `ExecutorService`
 * - Implement `ScheduledExecutorService`
 * - Use Micrometer for metric collection
 * - Return CompletableFuture instead of Future
 */
public class ThreadPool implements Executor {

    private static final Worker[] EMPTY_WORKERS_ARRAY = new Worker[0];
    private static final Runnable SHUTDOWN_TASK = () -> {};
    private final int minNumWorks;
    private final int maxNumWorks;
    private final long idleTimeoutNano;
    private final BlockingQueue<Runnable> queue = new LinkedTransferQueue<>();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final Set<Worker> workers = new HashSet<>();
    private final Lock workersLock = new ReentrantLock();
    private final AtomicInteger numWorks = new AtomicInteger();
    private final AtomicInteger numBusyWorks = new AtomicInteger();

    public ThreadPool(int minNumWorks, int maxNumWorks, Duration idleTimeout) {
        this.minNumWorks = minNumWorks;
        this.maxNumWorks = maxNumWorks;
        this.idleTimeoutNano = idleTimeout.toNanos();
    }

    private Worker newWorker(WorkerType workerType) {

        numWorks.incrementAndGet();
        numBusyWorks.incrementAndGet();

        final Worker worker = new Worker(workerType);
        workers.add(worker);

        return worker;
    }

    @Override
    public void execute(Runnable command) {
        if(shutdown.get()) {
            throw new RejectedExecutionException();
        }

        queue.add(command);
        addWorkersIfNecessary();

        if(shutdown.get()) {
            //noinspection ResultOfMethodCallIgnored
            queue.remove(command);
            throw new RejectedExecutionException();
        }
    }

    private void addWorkersIfNecessary() {
        if(needsMoreWorker() != null) {
            workersLock.lock();
            List<Worker> newWorkers = null;
            try {

                while (!shutdown.get()) {
                    final WorkerType workerType = needsMoreWorker();
                    if(workerType != null) {
                        if(newWorkers == null) {
                            newWorkers = new ArrayList<>();
                        }
                        newWorkers.add(newWorker(workerType));
                    } else {
                        break;
                    }
                }
            } finally {
                workersLock.unlock();
            }
            // Call 'Thread.start()' call out of the lock to minimize the contention.
            if (newWorkers != null) {
                newWorkers.forEach(Worker::start);
            }
        }
    }

    /**
     * Returns the type of the worker if more worker is needed to handle a newly submitted task.
     * {@code null} is returned if no new worker is needed.
     */
    @Nullable
    private WorkerType needsMoreWorker() {
        final int numBusyWorks = this.numBusyWorks.get();
        final int numWorks = this.numWorks.get();

        if(numWorks < minNumWorks) {
            return WorkerType.CORE;
        }

        if(numBusyWorks >= numWorks) {
            if(numBusyWorks < maxNumWorks) {
                return WorkerType.EXTRA;
            }
        }
        return null;
    }

    public void shutdown() {

        if (shutdown.compareAndSet(false, true)) {
            // wait until the queue is completely drained.
            for (int i = 0; i < maxNumWorks; i++) {
                queue.add(SHUTDOWN_TASK);
            }
        }

        //
        for (;;) {
            final Worker[] workers;
            workersLock.lock();
            try {
                workers = this.workers.toArray(EMPTY_WORKERS_ARRAY);
            } finally {
                workersLock.unlock();
            }

            if(workers.length == 0) {
                break;
            }

            for(Worker w : workers) {
                w.join();
            }
        }
    }

    private enum WorkerType {
        /**
         * The core worker that doesn't get terminated due to idle timeout.
         * It's only terminated by {@link #SHUTDOWN_TASK}, which the pool is shutdown.
         */
        CORE,
        /**
         * The worker that can be terminated due to idle timeout.
         */
        EXTRA
    }

    private class Worker {
        private final WorkerType type;
        private final Thread thread;

        Worker(WorkerType type) {
            this.type = type;
            thread = new Thread(this::work);
        }

        void start() {
            thread.start();
        }

        public void join() {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // Do not propagate to prevent incomplete shutdown.
                }
            }
        }

        private void work() {
            boolean isBusy = true;
            long lastRunTimeNanos = System.nanoTime();
            try {
                loop: for (;;) {
                    try {
                        Runnable task = queue.poll();
                        if (task == null) {
                            if( isBusy ) {
                                isBusy = false;
                                numBusyWorks.decrementAndGet();
                            }

                            switch (type) {
                                case CORE:
                                    // A core worker is never terminated by idle timeout. so we just wait forever.
                                    task = queue.take();
                                    break;
                                case EXTRA:
                                    final long waitTimeNanos =
                                            idleTimeoutNano - (System.nanoTime() - lastRunTimeNanos);
                                    if(waitTimeNanos <= 0 ||
                                            (task = queue.poll(waitTimeNanos, TimeUnit.NANOSECONDS)) == null ) {

                                        System.err.println(Thread.currentThread().getName() + " hit by idle timeout");
                                        break loop;
                                    }

                                    break;
                                default:
                                    throw new Error();
                            }



                            isBusy = true;
                            numBusyWorks.incrementAndGet();
                        } else {
                            if(!isBusy) {
                                isBusy = true;
                                numBusyWorks.incrementAndGet();
                            }
                        }

//                        final Runnable task = queue.take();
                        if(task == SHUTDOWN_TASK) {
                            System.err.println(Thread.currentThread().getName() + " received a poison pill");
                            break;
                        } else {
                            try {
                                task.run();
                            } finally {
                                lastRunTimeNanos = System.nanoTime();
                            }
                        }
                    } catch(Throwable t) {
                        if(!(t instanceof InterruptedException)) {
                            System.err.println("Unexpected exception: ");
                            t.printStackTrace();
                        }
                    }
                }
            } finally {
                workersLock.lock();
                try {
                    workers.remove(this);

                    numWorks.decrementAndGet();
                    numBusyWorks.decrementAndGet();   // Was busy handling the `SHUTDOWN_TASK`

                    if(workers.isEmpty() && !queue.isEmpty()) {
                        for (Runnable task : queue) {
                            if(task != SHUTDOWN_TASK) {
                                addWorkersIfNecessary();
                                break;
                            }
                        }
                    }
                } finally {
                    workersLock.unlock();
                }
                System.err.println("Shutting down thread '" + Thread.currentThread().getName() + "' (" + type + "'");
            }
        }
    }
}
