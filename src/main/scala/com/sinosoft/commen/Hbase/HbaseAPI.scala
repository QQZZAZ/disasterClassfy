package com.sinosoft.commen.Hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

object HbaseAPI {
  lazy val tablename = "WEIBO_INFO_TABLE"

  def getHBaseData(sc:SparkContext)={
    val conf = HBaseConfiguration.create()
    //这里的地址需要与Hbase-site.xml一一对应
    //hbase.zookeeper.quorum 填写的是zookeeper的集群地址，需要适配其中某一个地址，因为HBase是分配到具体zookeeper下来管理的
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    //    conf.set("hbase.zookeeper.quorum","192.168.120.83")
    conf.set("hbase.zookeeper.quorum","192.168.129.10,192.168.129.11,192.168.129.12")
//    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000)
    conf.set(TableInputFormat.INPUT_TABLE, tablename)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    println("读取到 "+hBaseRDD.count()+" 条数据")
    hBaseRDD
  }
  //重新获取getHBaseData之后的数据，并进行过滤，仅保留web_type_code 不为空的内容
  def updateHBaseData(sc:SparkContext)={
    val updateHBaseDataRDD = getHBaseData(sc)
    val pairRDD = updateHBaseDataRDD.map(f =>(Bytes.toString(f._2.getRow),
      (Bytes.toString(f._2.getValue("wa".getBytes,"web_info_content".getBytes))
        ,Bytes.toString(f._2.getValue("wa".getBytes,"web_type_code".getBytes))
      )
    ))
    val secondTransformRdd = pairRDD.filter(f=>f._2._2 != null)
    secondTransformRdd
  }
}
