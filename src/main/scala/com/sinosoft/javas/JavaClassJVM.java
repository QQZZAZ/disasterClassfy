package com.sinosoft.javas;

import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.ExecutionException;


/**
 * 查看创建对象在jvm堆中的大小
 */
public class JavaClassJVM {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        char o ='2';
        System.out.println("maureen test:" + ClassLayout.parseInstance(o).toPrintable());
    }
}
