package com.jake.threadpool;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

class ThreadPoolTest {

     @Test
     void submittedTasksAreExecuted() {
         final ThreadPool executor = new ThreadPool(100);
         final int numTasks = 100;
         final CountDownLatch latch = new CountDownLatch(numTasks);
         try {
             for(int i=0; i < 100; i++) {
                 final int finalI = i;
                 executor.execute(() -> {
                     System.err.println("Thread '" + Thread.currentThread().getName() + "' executes a task " + finalI);
                     try {
                         Thread.sleep(10);
                     } catch (InterruptedException e) {
                         //
                     }
                     latch.countDown();
                 });
             }
         } finally {
             executor.shutdown();
         }

     }
}
