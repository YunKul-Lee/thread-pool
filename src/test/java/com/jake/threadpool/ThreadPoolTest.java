package com.jake.threadpool;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

class ThreadPoolTest {

     @Test
     void submittedTasksAreExecuted() throws Exception {
         final ThreadPool executor = new ThreadPool(100, Duration.ofSeconds(1));
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

             latch.await();
//             Thread.sleep(100000);
         } finally {
             executor.shutdown();
         }

     }
}
