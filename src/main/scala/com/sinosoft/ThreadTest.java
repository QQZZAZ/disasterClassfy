package com.sinosoft;

import java.util.concurrent.*;
import org.openjdk.jol.info.ClassLayout;

public class ThreadTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        char o ='2';
        System.out.println("maureen test:" + ClassLayout.parseInstance(o).toPrintable());
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