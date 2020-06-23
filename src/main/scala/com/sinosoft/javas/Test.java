package com.sinosoft.javas;

import org.apache.spark.sql.sources.StreamSinkProvider;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static com.sinosoft.utils.ExcelReaderUtil.getAllFileName;

public class Test {
    public static void main(String[] args) throws Exception {
        ArrayList<String> list = new ArrayList<String>();
        getAllFileName("D:/data", list);
        PrintWriter pr = null;
        BufferedReader bufferReader= null;
        for (String file : list) {
            bufferReader = new BufferedReader(new InputStreamReader
                    (new FileInputStream(
                            new File(file)), "utf-8"));
            String message = "";
            message = bufferReader.readLine();
            StringBuffer sb = new StringBuffer();
            while (message != null) {
                sb.append(message);
                message = bufferReader.readLine();
            }
            bufferReader.close();
            pr = new PrintWriter(file);
            pr.write(sb.toString());
            pr.close();
        }
    }

    private static void get(List<String> str) {

        str.add("no");
        str.add("no");

        System.out.println(str);
    }
}
