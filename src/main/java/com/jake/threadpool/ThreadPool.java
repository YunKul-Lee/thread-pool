package com.jake.threadpool;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadPool implements Executor {

    private static final Thread[] EMPTY_THREADS_ARRAY = new Thread[0];
    private static final Runnable SHUTDOWN_TASK = () -> {};
    private final int maxNumThreads;
    private final long idleTimeoutNano;
    private final BlockingQueue<Runnable> queue = new LinkedTransferQueue<>();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final Set<Thread> threads = new HashSet<>();
    private final Lock threadsLock = new ReentrantLock();
    private final AtomicInteger numThreads = new AtomicInteger();
    private final AtomicInteger numActiveThreads = new AtomicInteger();

    public ThreadPool(int maxNumThreads, Duration idleTimeout) {
        this.maxNumThreads = maxNumThreads;
        this.idleTimeoutNano = idleTimeout.toNanos();
    }

    private Thread newThread() {

        numThreads.incrementAndGet();
        numActiveThreads.incrementAndGet();

        final Thread thread = new Thread(() -> {
            boolean isActive = true;
            long lastRunTimeNanos = System.nanoTime();
            try {
                for (;;) {
                    try {
                        Runnable task = queue.poll();
                        if (task == null) {
                            if( isActive ) {
                                isActive = false;
                                numActiveThreads.decrementAndGet();
                            }

                            final long waitTimeNanos = idleTimeoutNano - (System.nanoTime() - lastRunTimeNanos);

                            if(waitTimeNanos <= 0) {
                                break;
                            }

                            task = queue.poll(waitTimeNanos, TimeUnit.NANOSECONDS);
                            if(task == null) {
                                break;
                            }
                            isActive = true;
                            numActiveThreads.incrementAndGet();
                        } else {
                            if(!isActive) {
                                isActive = true;
                                numActiveThreads.incrementAndGet();
                            }
                        }

//                        final Runnable task = queue.take();
                        if(task == SHUTDOWN_TASK) {
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
                threadsLock.lock();
                try {
                    threads.remove(Thread.currentThread());
                } finally {
                    threadsLock.unlock();
                }
                numThreads.decrementAndGet();
                if (isActive) {
                    numActiveThreads.decrementAndGet();
                }
            }
            System.err.println("Shutting down thread '" + Thread.currentThread().getName() + '\'');

        });
        threads.add(thread);

        return thread;
    }

    @Override
    public void execute(Runnable command) {
        if(shutdown.get()) {
            throw new RejectedExecutionException();
        }

        queue.add(command);
        addThreadIfNecessary();


        if(shutdown.get()) {
            queue.remove(command);
            throw new RejectedExecutionException();
        }
    }

    private void addThreadIfNecessary() {
        if(needsMoreThreads()) {
            threadsLock.lock();
            Thread newThread = null;
            try {
                if(needsMoreThreads() && !shutdown.get()) {
                    newThread = newThread();
                }
            } finally {
                threadsLock.unlock();
            }
            // Call 'Thread.start()' call out of the lock to minimize the contention.
            if (newThread != null) {
                newThread.start();
            }
        }
    }

    private boolean needsMoreThreads() {
        final int numActiveThreads = this.numActiveThreads.get();
        final int numThreads = this.numThreads.get();
        return numActiveThreads < maxNumThreads && numActiveThreads >= numThreads;
    }

    public void shutdown() {

        if (shutdown.compareAndSet(false, true)) {
            // wait until the queue is completely drained.
            for (int i=0; i < maxNumThreads; i++) {
                queue.add(SHUTDOWN_TASK);
            }
        }

        //
        for (;;) {
            final Thread[] threads;
            threadsLock.lock();
            try {
                threads = this.threads.toArray(EMPTY_THREADS_ARRAY);
            } finally {
                threadsLock.unlock();
            }

            if(threads.length == 0) {
                break;
            }

            for(Thread thread : threads) {
                do {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        // Do not propagate to prevent incomplete shutdown.
                    }
                } while (thread.isAlive());
            }
        }
    }
}
