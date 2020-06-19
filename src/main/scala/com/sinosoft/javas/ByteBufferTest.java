package com.sinosoft.javas;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LinkedBlockingQueue 多用于任务队列
 * ConcurrentLinkedQueue 多用于消息队列
 * <p>
 * 单生产者，单消费者 用 LinkedBlockingqueue
 * 多生产者，单消费者 用 LinkedBlockingqueue
 * 单生产者 ，多消费者 用 ConcurrentLinkedQueue
 * 多生产者 ，多消费者 用 ConcurrentLinkedQueue
 */
public class ByteBufferTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        /*FileWrite fw = new FileWrite();
        new Thread(() -> {
            fw.run();
        }, "A").start();*/


       /* ByteBuffer bf = ByteBuffer.allocateDirect(1024);

        Configuration conf = new Configuration();
        //这里指定使用的是 hdfs文件系统
        conf.set("fs.defaultFS", "hdfs://master:9000");

        //通过这种方式设置java客户端身份
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(conf);*/
        //或者使用下面的方式设置客户端身份
        //FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");
        // fs.create(new Path("/helloByJava")); //创建一个目录

        //文件下载到本地 如果出现0644错误或找不到winutils.exe,则需要设置windows环境和相关文件.
        //fs.copyToLocalFile(new Path("/zookeeper.out"), new Path("D:\\test\\examplehdfs"));

        FileQueueEntity fqe = FileQueueEntity.getInstance();
        //使用Stream的形式操作HDFS，这是更底层的方式
       /* FSDataOutputStream outputStream = fs.create(new Path("/2.txt"), true); //输出流到HDFS
        FileInputStream inputStream = new FileInputStream("D:/test/examplehdfs/1.txt"); //从本地输入流。
        IOUtils.copy(inputStream, outputStream); //完成从本地上传文件到hdfs

        fs.close();*/

        new Thread(() -> {
            int j = 0;
            LinkedBlockingQueue<String[]> qu = fqe.getFileQueue();
            while (j < 500001) {
                String[] arr = new String[4];
                arr[0] = j + "a";
                arr[1] = j + "b";
                arr[2] = j + "c";
                arr[3] = j + "d";
                try {
                    qu.put(arr);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                fqe.addFileQueueCount();
                j ++;
            }
        }, "B").start();

        new Thread(() -> {
            int j = 0;
            LinkedBlockingQueue<String[]> qu = fqe.getFileQueue();
            while (j < 50000000) {
                String[] arr = new String[4];
                arr[0] = j + "a1";
                arr[1] = j + "b1";
                arr[2] = j + "c1";
                arr[3] = j + "d1";
                try {
                    qu.put(arr);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                fqe.addFileQueueCount();
                j ++;
            }
        }, "C").start();

    }
}
