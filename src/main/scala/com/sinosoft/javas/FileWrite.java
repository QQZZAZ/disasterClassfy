package com.sinosoft.javas;

import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;

public class FileWrite implements Runnable {
    private volatile FileQueueEntity fqe = FileQueueEntity.getInstance();
    private volatile LinkedBlockingQueue<String[]> qu = fqe.getFileQueue();
    private volatile PrintWriter pw = new PrintWriter("D:/1.txt");
    final File logFile = new File("D:/1.txt");
    private Writer txtWriter = null;


    public FileWrite() throws IOException {

    }

    @Override
    public void run() {
        int i = 0;
        while (true) {
            while (fqe.getFileQueueCount() >= 1000) {
                try {
                    txtWriter = new FileWriter(logFile, true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                fqe.decFileQueueCount();
                while (i < 1000) {
                    String[] arr = new String[0];
                    try {
                        arr = qu.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        txtWriter.write(arr[0] + "\tab" + arr[1] + "\tab" + arr[2] + "\tab" + arr[3] + "\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    i += 1;
                }
                try {
                    txtWriter.flush();
                    txtWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                i = 0;
            }
            try {
                Thread.sleep(5000);
                System.out.println("还剩" + fqe.getFileQueueCount());

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
