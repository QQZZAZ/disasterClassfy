//package com.sinosoft
//
//import java.sql.Timestamp
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import com.google.common.base.Stopwatch
//import com.sinosoft.utils.MyUtil
//import kafka.utils.ZkUtils
//import org.I0Itec.zkclient.ZkClient
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
//import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
//import org.apache.spark.streaming.kafka010.HasOffsetRanges
//import org.apache.zookeeper.ZKUtil
//
//object KafkaEverydayCount {
//
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val spark = SparkSession.builder()
//      .appName("KafkaEverydayCount").master("local")
//      .config("spark.default.parallelism", 100)
//      .getOrCreate()
//    import spark.implicits._
//
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "192.168.129.11:9092,192.168.129.12:9092")
//      .option("subscribe", "dblab")
//      //从kafka的每个partition最多只取20000条数据，当服务器宕机时使用，因为会积压大量数据
//      //      .option("spark.streaming.kafka.maxRatePerPartition",20000)
//      //      .option("spark.streaming.backpressure.enabled",true) //开启内部背压机制与上面的maxRatePerPartition组合使用
//      .load()
//    val words = df.select("value", "timestamp")
//      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").toDF("word", "timestamp")
//    /*.as[(String,Timestamp)].map(word => (word._1, word._2))
//  .toDF("word", "timestamp")*/
//    // Group the data by window and word and compute the count of each group
//    //设置窗口大小和滑动窗口步长
//    val windowedCounts = words.as[(String, Timestamp)]
//      .withWatermark("timestamp", "24 hours")
//      .groupBy(
//      functions.window($"timestamp", "10 seconds", "5 seconds"), $"word"
//    ).count()
//
//    //todo 设置checkpoint
//    /*val qu = q.writeStream
//      .outputMode("append").format("text").option("path","/test/zdb/result")
//      .option("checkpointLocation", "/test/zdb/")
//      .trigger(Trigger.ProcessingTime(
//        80000
//      )).start()*/
//
//    val qu = windowedCounts.writeStream
//      .outputMode("complete").format("console")
//      .option("truncate",false)
//      /*.trigger(Trigger.ProcessingTime(
//        1000
//      ))*/.start()
//    qu.awaitTermination()
//  }
//
//
//
//}
