package com.sinosoft.javas;

import java.io.Serializable;

public class HashAl implements Serializable {
    public static int RSHash(String str, int reg) {

        long b = 37851;
        long a = 636;
        long hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash = hash * a + str.charAt(i);
            a = a * b;
        }

        long num = (hash & 0xffffffffffffffffl);
        return (int) (Math.abs(num) % reg);
    }

    public static long getHash(String str) {

        long b = 37851;
        long a = 636;
        long hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash = hash * a + str.charAt(i);
            a = a * b;
        }

        long num = (hash & 0xffffffffffffffffl);
        return Math.abs(num);
    }

    public static void main(String[] args) {
        long a = RSHash("https://search.bilibili.com/all?keyword=java%20RPC&from_source=nav_search&spm_id_from=333.851.b_696e7465726e6174696f6e616c486561646572.10", 49);
        long b = RSHash("https://www", 49);

        System.out.println(a);
        System.out.println(b);


//        System.out.println(Math.abs(a) % 299);
    }

}
