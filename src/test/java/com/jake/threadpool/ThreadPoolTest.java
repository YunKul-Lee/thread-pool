package com.jake.threadpool;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executor;

class ThreadPoolTest {

     @Test
     void submittedTasksAreExecuted() {
         final Executor executor = new ThreadPool(2);

         for(int i=0; i < 10; i++) {
             final int finalI = i;
             executor.execute(() -> {
                 System.out.println("Thread '" + Thread.currentThread().getName() + "' executes a task " + finalI);

             });
         }
     }
}
