package com.sinosoft.javas;

public class HashAl {
    public static long RSHash(String str) {

        long b = 37851;
        long a = 636;
        long hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash = hash * a + str.charAt(i);
            a = a * b;
        }
        return (hash & 0xffffffffffffffffl);
    }

    public static void main(String[] args) {
        long a = RSHash("我和我的祖国abs");

        System.out.println(Math.abs(a) % 299);
    }

}
