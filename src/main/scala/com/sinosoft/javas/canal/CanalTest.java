package com.sinosoft.javas.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.net.InetSocketAddress;

public class CanalTest {
    private static String SERVER_ADDRESS = "192.168.10.128";

    private static Integer PORT = 11111;

    //Canal内部存储binlog的队列名字，默认叫example，在my.conf中默认配置 canal.destinations
    private static String DESTINATION ="example";

    //server没有用户名和密码，空串就可以
    private static String USERNAME ="";

    private static String PASSWORD ="";

    public static void main(String[] args) {
        CanalConnector canalConnector =  CanalConnectors.newSingleConnector(
                new InetSocketAddress(SERVER_ADDRESS,PORT),
                DESTINATION,
                USERNAME,
                PASSWORD
        );
        canalConnector.connect();
        //做订阅
        canalConnector.subscribe(".*\\..*");
        //恢复到之前的同步位置
        canalConnector.rollback();
    }
}
