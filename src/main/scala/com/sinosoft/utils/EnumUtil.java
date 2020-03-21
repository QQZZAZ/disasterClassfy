package com.sinosoft.utils;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by guo on 2017/11/7.
 */
public class EnumUtil {

    public static Properties property;

    static {
        property = new Properties();
        try {
            property.load(EnumUtil.class.getClassLoader().getResourceAsStream("application.properties"));
            String path = property.getProperty("profiles.active");
            System.out.println("path: "+ path);

            property.load(EnumUtil.class.getClassLoader().getResourceAsStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    //hbase zookeeper地址
    public static final String HBASE_ZOOKEEPER_IP = property.getProperty("hbase.zookeeper.ip");//"10.10.30.3,10.10.30.4,10.10.30.19,10.10.30.20,10.10.30.21";
    public static final String TJHBASE_ZOOKEEPER_IP = property.getProperty("tjhbase.zookeeper.ip");

    public static final String TEST_IPURL = "10.20.30.47,10.20.30.48,10.20.30.49";
    //hbase的master
    public static final String HBASE_MASTER = property.getProperty("hbase.master");
    public static final String TJHBASE_MASTER = property.getProperty("tjhbase.master");
    //hbase的zookeeper的端口
    public static final String HBASE_ZOOKEEPER_PORT = "2181";
    //kafka zookeeper地址
    public static final String KAFKA_ZOOKEEPER_URL = property.getProperty("KAFKA_ZOOKEEPER_URL");

    public static final String redisUrl = property.getProperty("redis.url");

    public static final int redisPost = Integer.parseInt(property.getProperty("redis.post"));

    /*//kafka的groupid
    public static final String GROUP_ID = "text_analysis_spark_ge";
    //mysql的url
    public static final String MYSQL_URL = property.getProperty("mysql.url");
    //mysql的用户名
    public static final String MYSQL_USERNAME = property.getProperty("mysql.username");
    //mysql的密码
    public static final String MYSQL_PASSWORD = property.getProperty("mysql.password");
    //redis地址
    public static final String redisUrl = property.getProperty("redis.url");
    //redis端口
    public static final int redisPost = Integer.parseInt(property.getProperty("redis.post"));
    //天津redis地址
    public static final String tjredisUrl = property.getProperty("tjredis.url");
    //天津redis端口
    public static final int tjredisPost = Integer.parseInt(property.getProperty("tjredis.post"));
    //redis集群地址
    public static final String redisClusterUrl = property.getProperty("redisCluster.url");
    //redis集群端口
    public static final int redisClusterPost = Integer.parseInt(property.getProperty("redisCluster.post"));
    //连接elasticsearch证书路径
    public static final String CERTIFICATE_PATH = property.getProperty("certificate.path");

    //微博表的redis队列
    public static final String WEIBO_INFO_REDIS_KEY = property.getProperty("weibo_info_redis_key");
    public static final String WEIBO_TRANSFER_REDIS_KEY = property.getProperty("weibo_transfer_redis_key");
    public static final String WEIBO_COMMENT_REDIS_KEY = property.getProperty("weibo_comment_redis_key");
    public static final String WEIBO_LIKE_REDIS_KEY = property.getProperty("weibo_like_redis_key");
    public static final String WEIBO_USER_REDIS_KEY = property.getProperty("weibo_user_redis_key");
    public static final String WEIBO_FANS_REDIS_KEY = property.getProperty("weibo_fans_redis_key");
    public static final String WEIBO_FOLLOWER_REDIS_KEY = property.getProperty("weibo_follower_redis_key");
    //微信表的redis队列
    public static final String WECHAT_INFO_REDIS_KEY = property.getProperty("wechat_info_redis_key");
    public static final String WECHAT_COMMENT_REDIS_KEY = property.getProperty("wechat_comment_redis_key");
    public static final String WECHAT_USER_REDIS_KEY = property.getProperty("wechat_user_redis_key");
    public static final String WECHAT_GROUP_REDIS_KEY = property.getProperty("wechat_group_redis_key");
    //新闻表的redis队列
    public static final String NEWS_INFO_REDIS_KEY = property.getProperty("news_info_redis_key");
    public static final String NEWS_COMMENT_REDIS_KEY = property.getProperty("news_comment_redis_key");
    public static final String NEWS_PERSON_REDIS_KEY = property.getProperty("news_person_redis_key");
    public static final String NEWS_ORGIN_REDIS_KEY = property.getProperty("news_orgin_redis_key");
    //论坛表的redis队列
    public static final String FORUM_INFO_REDIS_KEY = property.getProperty("forum_info_redis_key");
    //贴吧表的redis队列
    public static final String TIEBA_INFO_REDIS_KEY = property.getProperty("tieba_info_redis_key");
    public static final String TIEBA_PEO_REDIS_KEY = property.getProperty("tieba_peo_redis_key");
    //抖音表的redis队列
    public static final String DOUYIN_INFO_REDIS_KEY = property.getProperty("douyin_info_redis_key");
    public static final String DOUYIN_USER_REDIS_KEY = property.getProperty("douyin_user_redis_key");
    public static final String DOUYIN_COMMENT_REDIS_KEY = property.getProperty("douyin_comment_redis_key");
    //SITE_RECORD表的redis队列
    public static final String SITE_RECORD_REDIS_KEY = property.getProperty("site_record_redis_key");
    //统计报表的redis队列
    public static final String COUNT_INFO_REDIS_KEY = property.getProperty("count_info_redis_key");
    //微博Info表的redis删除队列
    public static final String WEIBO_INFO_DEL_REDIS_KEY = property.getProperty("weibo_info_del_redis_key");
    //微博转发表的redis删除队列
    public static final String WEIBO_TRANSFER_DEL_REDIS_KEY = property.getProperty("weibo_transfer_del_redis_key");
    //微博评论表的redis删除队列
    public static final String WEIBO_COMMENT_DEL_REDIS_KEY = property.getProperty("weibo_comment_del_redis_key");
    //微博点赞表的redis删除队列
    public static final String WEIBO_LIKE_DEL_REDIS_KEY = property.getProperty("weibo_like_del_redis_key");
    //微博粉丝表的redis删除队列
    public static final String WEIBO_FANS_DEL_REDIS_KEY = property.getProperty("weibo_fans_del_redis_key");
    //微博关注表的redis删除队列
    public static final String WEIBO_FOLLOWER_DEL_REDIS_KEY = property.getProperty("weibo_follower_del_redis_key");
    //微博的重复HanLP取词数量
    public static final String REPEATED_HANLP_WEIBO_WEIGHTS = property.getProperty("repeated_hanlp_weibo_weights");*/

}