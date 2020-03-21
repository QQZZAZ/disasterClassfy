package com.sinosoft.scheduler

import java.util.regex.Pattern

import com.sinosoft.commen.Hbase.HbaseAPI
import com.sinosoft.utils.MyUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object DisatserService {
  def updateHistoryData(sc:SparkContext,regData: Broadcast[scala.collection.mutable.HashMap[String,scala.collection.mutable.ArrayBuffer[Tuple3[String,String,String]]]],
                        hBaseRDD: RDD[(ImmutableBytesWritable,Result)]): Unit ={
    val tablename = "INFO_TABLE"
    val conf = HBaseConfiguration.create()

    val jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.property.clientPort", "2181")
    jobConf.set("hbase.zookeeper.quorum","192.168.129.12")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    val pairRDD = hBaseRDD.map(f =>(Bytes.toString(f._2.getRow),
      (Bytes.toString(f._2.getValue("wa".getBytes,"web_info_content".getBytes))
        ,Bytes.toString(f._2.getValue("wa".getBytes,"web_type_code".getBytes))
      )
    ))
    /**
      * 将RDD转为DataFrame，然后进行sql操作
      * val spark = SparkSession.builder()
      * .master("local")
      * .appName("RDD2DataFrameReflection")
      * .getOrCreate();
      * val sqlContext = spark.sqlContext
      * import sqlContext.implicits._
      * val pairRDD = hBaseRDD.map(f =>(Bytes.toString(f._2.getRow),
      * (Bytes.toString(f._2.getValue("wa".getBytes,"web_info_content".getBytes))
      * ,Bytes.toString(f._2.getValue("wa".getBytes,"web_type_code".getBytes))
      * )
      * )).toDF
      */

    println("RDD数量："+pairRDD.count())

    pairRDD.map(x =>{
      var i = 0
      val put = new Put(Bytes.toBytes(x._1))
      val str = x._2._1
      var code = ""
      var rule = ""
      //todo
      for((tupo1,tupo2) <- regData.value) {
        if(x._2._2 != null && !x._2._2.equals("") && str.contains(tupo1) ){
          for(reg <- tupo2){
            val pattern = Pattern.compile(reg._1)
            println("reg._1"+reg._1)
            val matcher = pattern.matcher(str)
            //这里打桩输出了5次结果，为什么？？？？
            if (matcher.find){
              //println("匹配到多个词"+reg._1)
              //需要将HBase中添加type标签并进行打标赋值
              if(!code.equals("") && !code.contains(reg._2)){
                code = code +","+reg._2
              }else{
                code = reg._2
              }
              if(!rule.equals("")){
                rule = rule +","+ reg._3
              }else{
                rule = reg._3
              }
              put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("web_type_code"), Bytes.toBytes(reg._2))
              put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("web_type_rules"), Bytes.toBytes(rule))
            }
          }
        }
      }
      (new ImmutableBytesWritable,put)
    }).filter(f=>{!f._2.isEmpty}).saveAsHadoopDataset(jobConf)
  }
  def setHistoryData2Redis(sc:SparkContext): Unit ={
    val rdd = HbaseAPI.updateHBaseData(sc)
    rdd.foreachPartition(it=> {
      val jedis = new Jedis("192.168.129.14")
      //      val jedis = new Jedis("localhost")
      it.foreach(data => {
        val key = data._1 + "|||||web_type_code"
        //        println(key)
        jedis.lpush("es:news:insert:info", key)
      })
    })
  }
  def quaryData(sc:SparkContext,
                regData: Broadcast[scala.collection.mutable.HashMap[String,scala.collection.mutable.ArrayBuffer[Tuple3[String,String,String]]]],
                hBaseRDD: RDD[(ImmutableBytesWritable,Result)]): Unit ={
    val timetmp = MyUtils.timeTransformation("2018-12-25 00:00:00")
    val pairRDD = hBaseRDD.map(f =>(Bytes.toString(f._2.getRow),
      (Bytes.toLong(f._2.getValue("wa".getBytes,"web_info_spider_time".getBytes))),
      (Bytes.toString(f._2.getValue("wa".getBytes,"web_type_code".getBytes)))
    )).filter(f=> {f._2 > timetmp})

  }
}
