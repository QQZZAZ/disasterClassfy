package com.sinosoft.javas;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 枚举单例
 *
 * @author fengyr
 */
public class FileQueueEntity {

    /**
     * 单例对象
     */
    private volatile static FileQueueEntity instance = null;

    /**
     * 缓存队列
     */
    private volatile LinkedBlockingQueue<String[]> FileCache = new LinkedBlockingQueue(10000);

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