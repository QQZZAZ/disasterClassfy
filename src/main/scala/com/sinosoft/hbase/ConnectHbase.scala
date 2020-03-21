package com.sinosoft.hbase

import com.sinosoft.utils.EnumUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by guo on 2018/4/18.
  */
object ConnectHbase{

  /**
    * 创建初始的rdd
    * @param sc
    * @param conf
    * @return
    */
  def create_Initial_RDD(sc:SparkContext,conf:Configuration):RDD[(ImmutableBytesWritable, Result)]={
    val initialRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    initialRDD
  }

  /**
    * 创建JobConf对象
    * @param tableName
    * @param conf
    * @return
    */
  def createJobConf(tableName:String,conf:Configuration): JobConf = {
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf
  }

  /**
    * 创建configuration
    * @param tableName
    * @return
    */
  def hbase_conf(tableName:String): Configuration = {
    val conf = HBaseConfiguration.create()
    //设置zookeeper集群地址
    conf.set("hbase.zookeeper.quorum", EnumUtil.HBASE_ZOOKEEPER_IP)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", EnumUtil.HBASE_ZOOKEEPER_PORT)
    //设置hbase的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    //    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000)
    conf
  }
}
