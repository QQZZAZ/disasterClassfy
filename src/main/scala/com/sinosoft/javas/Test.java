package com.sinosoft.javas;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        String a = "ok|1";
        String b = "ok|12";
        if (b.compareTo(a) <0 ) {
            System.out.println("ok");
        }else{
            System.out.println("over");
        }
    }

    private static void get(List<String> str) {

        str.add("no");
        str.add("no");

        System.out.println(str);
    }
}
