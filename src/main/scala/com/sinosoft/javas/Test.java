package com.sinosoft.javas;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args){
     String a = "ok";
     String b = "ok";
     System.out.println(a==b);
     List<String> list = new ArrayList();
     get(list);
     System.out.println(list.size());
    }

    private static void get(List<String> str){

        str.add("no");
        str.add("no");

        System.out.println(str);
    }
}
