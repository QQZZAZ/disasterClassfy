package com.sinosoft.javas;

import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 枚举单例
 *
 * @author fengyr
 */
public class FileQueueEntity implements Runnable, Serializable {
    private Writer txtWriter = null;
    final File logFile = new File("D:/1.txt");

    static {
        FileQueueEntity fqe = FileQueueEntity.getInstance();
        new Thread(() -> {
            fqe.run();
        }, "A").start();
    }

    /**
     * 单例对象
     */
    private volatile static FileQueueEntity instance = null;

    /**
     * 缓存队列
     */
    private volatile LinkedBlockingQueue<String[]> FileCache = new LinkedBlockingQueue(20000);

    /**
     * 缓存的byte数，120M一个文件
     */
    private volatile AtomicInteger count = new AtomicInteger();


    /**
     * 私有构造方法保护对象
     */
    private FileQueueEntity() {

    }

    /**
     * 私有内部枚举类实现单例
     *
     * @return
     */
    public static FileQueueEntity getInstance() {
        return Singleton.INSTANCE.getInstance();
    }

    public LinkedBlockingQueue<String[]> getFileQueue() {
        return FileCache;
    }

    public int getFileQueueCount() {
        return count.get();
    }

    public int addFileQueueCount() {
        return count.getAndAdd(1);
    }

    public int decFileQueueCount() {
        return count.getAndAdd(-1000);
    }

    @Override
    public void run() {
        int i = 0;
        while (true) {
            while (getFileQueueCount() >= 1000) {
                try {
                    txtWriter = new FileWriter(logFile, true);
                    decFileQueueCount();
                    while (i < 1000) {
                        String[] arr = new String[0];
                        try {
                            arr = FileCache.take();
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
                    txtWriter.flush();
                    txtWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                i = 0;
            }
            try {
                Thread.sleep(5000);
                System.out.println("还剩" + getFileQueueCount());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private enum Singleton {
        INSTANCE;

        private FileQueueEntity singleton;

        // JNM会保证这个方法只调用一次
        Singleton() {
            singleton = new FileQueueEntity();

        }

        public FileQueueEntity getInstance() {
            return singleton;
        }
    }

}