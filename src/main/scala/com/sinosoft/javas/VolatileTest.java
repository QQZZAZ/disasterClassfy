package com.sinosoft.javas;

public class VolatileTest {

    public static void main(String[] args) throws InterruptedException {
        JoinThread j1 = new JoinThread();
        new Thread(() -> {

//                    JoinThread j = new JoinThread();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            j1.run();
            System.out.println(Thread.currentThread().getName() + " " + j1.n);


        }, "A").start();

        System.out.println("ok" + j1.n);

        while (j1.n == 0) {
        }

        System.out.println("over");

    }
}
