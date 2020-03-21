//package com.sinosoft
//
//import com.sinosoft.commen.Hbase.HbaseAPI
//import org.apache.spark.{SparkConf, SparkContext}
//import com.sinosoft.commen.mysql.MysqlAPI
//import com.sinosoft.scheduler.DisatserService
//import com.sinosoft.utils._
//import org.apache.hadoop.hbase.util.Bytes
//
//object MainClass {
//  def main(args: Array[String]): Unit = {
//    val timetmp = MyUtils.timeTransformation("2018-12-25 00:00:00")
//    println(timetmp)
//    val conf = new SparkConf().setMaster("local[*]").setAppName("DisasterClassify")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    val sc = new SparkContext(conf)
//    //1.获取mysql的数据
//    val mysqlAPI = new MysqlAPI
//    val mysqlData = mysqlAPI.MysqlAPI.getMysqlData()
//    //2.需要将mysql的k:正则表达式，v:标签，入到map中
//    val transformData = new TransformData
//    val map = transformData.registMysql(mysqlData)
//    //3.将map声明为广播变量，减少压力，优化性能
//    val mapBroadCast = sc.broadcast(map)
//    //4.获取HBase数据
//    val HbaseRDD = HbaseAPI.getHBaseData(sc)
//
////    val pairRDD = HbaseRDD.map(f =>(Bytes.toString(f._2.getRow),
////      (Bytes.toString(f._2.getValue("wa".getBytes,"web_info_content".getBytes))
////        ,Bytes.toString(f._2.getValue("wa".getBytes,"web_type_code".getBytes))
////      )
////    ))
////
////    pairRDD.foreach(f=>println(f._1+": "+f._2._1+", "+f._2._2))
//
//    //5.将广播变量与HBase数据提交到worker上运行
//    DisatserService.updateHistoryData(sc,mapBroadCast,HbaseRDD)
//    //6.向redis的指定list添加rowKey
//    DisatserService.setHistoryData2Redis(sc)
//  }
//}
