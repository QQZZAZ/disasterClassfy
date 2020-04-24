package com.sinosoft;

import java.util.concurrent.*;

public class ThreadTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        /*ThreadDemo td = new ThreadDemo();

        FutureTask<String> result = new FutureTask<String>(td);
        new Thread(result).start();

        String result2 = result.get();
        System.out.println(result2);*/

        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            try {
                Thread.sleep(index * 61000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cachedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName()+"");
                }
            });
        }
        cachedThreadPool.shutdown();

    }


}
class ThreadDemo implements Callable<String> {

    @Override
    public String call() throws Exception {
        int sum = 0;

        for (int i = 0; i <= 2; i++) {
            sum += i;
        }

        return sum+"";
    }
}