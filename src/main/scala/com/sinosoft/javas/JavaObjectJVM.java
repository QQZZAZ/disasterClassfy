package com.sinosoft.javas;

import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

/**
 * 利用openjdk的JOL工具
 * 查看创建的对象在java堆中占多大的内存
 */
public class JavaObjectJVM {
    public static void main(String[] args) {
        char o ='2';
        String str = "石头人真垃圾阿三大苏打倒萨倒萨高房价哈桑JFK屡屡反馈";
        List<String> list  = new ArrayList<>();
        System.out.println("maureen test:" + ClassLayout.parseInstance(JavaObjectJVM.class).toPrintable());
    }
}
