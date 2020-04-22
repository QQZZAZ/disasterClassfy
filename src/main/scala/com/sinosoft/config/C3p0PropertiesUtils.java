package com.sinosoft.config;

import com.sinosoft.utils.EnumUtil;

import java.io.IOException;
import java.util.Properties;

public class C3p0PropertiesUtils {
    public static Properties property;

    static {
        property = new Properties();
        try {
            property.load(EnumUtil.class.getClassLoader().getResourceAsStream("localc3p0.properties"));
            String path = property.getProperty("profiles.active");
            System.out.println("path: "+ path);

            property.load(EnumUtil.class.getClassLoader().getResourceAsStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    //mysql的用户名
    public static final String MYSQL_URL = property.getProperty("mysql.jdbc.url");
    //mysql的密码
    public static final String MYSQL_USERNAME = property.getProperty("mysql.username");
    //redis地址
    public static final String MYSQL_PASSWORD = property.getProperty("mysql.password");
    //redis端口
    public static final String MYSQL_MINPOOLSIZE = property.getProperty("mysql.pool.jdbc.minPoolSize");
    //redis集群地址
    public static final String MYSQL_MAXPOOLSIZE = property.getProperty("mysql.pool.jdbc.maxPoolSize");
    //redis集群端口
    public static final String MYSQL_INITIALPOOLSIZE = property.getProperty("mysql.pool.jdbc.initialPoolSize");
    //连接elasticsearch证书路径
    public static final String MYSQL_DRIVER = property.getProperty("mysql.driver");
    //连接elasticsearch证书路径
    public static final String MYSQL_MAXIDLETIME = property.getProperty("mysql.pool.jdbc.maxIdleTime");
    //连接elasticsearch证书路径
    public static final String MYSQL_ACQUIREINCREMENT = property.getProperty("mysql.pool.jdbc.acquireIncrement");


}
