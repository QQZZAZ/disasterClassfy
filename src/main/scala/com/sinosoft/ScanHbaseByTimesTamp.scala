package com.sinosoft

import java.io.{ByteArrayOutputStream, DataOutputStream}

import com.sinosoft.commen.Hbase.HbaseAPI.tablename
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.json.JSONObject

/**
  * 根据Hbase入库时间戳获取数据
  */
object ScanHbaseByTimesTamp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("ScanHbaseTest").master("local[*]")
      .getOrCreate()
    val startRowkey = ""
    val endRowkey = ""
    //开始rowkey和结束一样代表精确查询某条数据  
    //组装scan语句  
    val scan = new Scan()
    scan.setCacheBlocks(false)
    scan.setColumnFamilyTimeRange(Bytes.toBytes("wa"),1563323916066l,1577523707000l)

    val conf = HBaseConfiguration.create()
    //这里的地址需要与Hbase-site.xml一一对应
    //hbase.zookeeper.quorum 填写的是zookeeper的集群地址，需要适配其中某一个地址，因为HBase是分配到具体zookeeper下来管理的
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    //    conf.set("hbase.zookeeper.quorum","192.168.120.83")
    conf.set("hbase.zookeeper.quorum","192.168.129.231,192.168.129.232,192.168.129.233")
    //    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000)
    conf.set(TableInputFormat.INPUT_TABLE, "WEIBO_TMP_TABLE")
    val scan_str= convertScanToString(scan)
    conf.set(TableInputFormat.SCAN,scan_str)

    val sc = spark.sparkContext
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    println("读取到 "+hBaseRDD.count()+" 条数据")

    val rdd = hBaseRDD.map{case (_,result) => {
      val rowkey = Bytes.toString(result.getRow)
      val map = result.rawCells().map(x => {
        val key = new String(CellUtil.cloneQualifier(x))
        var value = new String(CellUtil.cloneValue(x))
        (key,value)
      })
      val wb_content = if (result.getValue("wa".getBytes, "wb_content".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "wb_content".getBytes)) else ""

      (rowkey,map,wb_content)
    }}/*.filter(f=>{
      val flag = false
      if
      flag
    })*/.map(x => {
      val rowkey = x._1
      val map = x._2
      val jo = new JSONObject()
      jo.put("rowkey",rowkey)
      map.foreach(y => {
        if(y._2 != null){
          jo.put(y._1,y._2)
        }
      })
      jo.toString()
    }).repartition(10).saveAsTextFile("hdfs://192.168.129.231:9000/zdb/beijingfile")

  }

  def convertScanToString(scan:Scan):String ={
    val proto = ProtobufUtil.toScan(scan)
    return Base64.encodeBytes(proto.toByteArray)
  }
}
