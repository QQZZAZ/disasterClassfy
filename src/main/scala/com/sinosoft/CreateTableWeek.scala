package com.sinosoft

import java.sql.Timestamp
import java.util.regex.Pattern

import com.sinosoft.sink.{DriverHeapMysqlSink, ExecutorHeapMysqlSink, HbaseSink}
import com.sinosoft.utils.MyUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.json.JSONObject

object CreateTableWeek {
  lazy val xjkafkaurl = "10.10.30.131:9092,10.10.30.132:9092,10.10.30.133:9092,10.10.30.134:9092,10.10.30.135:9092"
  lazy val tjkafkaurl = "192.168.129.11:9092,192.168.129.12:9092"

  case class Event(sessionId: String, timestamp: Timestamp)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("CreateTableWeekCheckTest2").master("local[*]")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //    spark.streams.addListener()
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", tjkafkaurl)
      .option("subscribe", "spark_log_statistics")
      //从kafka的每个partition最多只取20000条数据，当服务器宕机时使用，因为会积压大量数据
      .option("spark.streaming.backpressure.enabled", true) //开启内部背压机制与上面的maxRatePerPartition组合使用
      .option("spark.speculation", true)
      //      .option("endingoffsets", "latest")  这个参数不支持
      .load()

    //    df.printSchema()
    val qa = df.select("value")
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(f => {
        val js = new JSONObject(f)
        val key = js.getString("source")
        val message = js.getString("message")
        val fields = js.getString("fields")
        val time = js.getString("@timestamp")
        (key, message, fields, time)
      })

    val driverheapDF = qa.filter(f => {
      val index = f._1.lastIndexOf("/")
      var rowkey = f._1.substring(index + 1)
      val pattern = Pattern.compile("application_.*\\.driver\\.jvm\\.heap\\.used\\.csv")
      val matcher = pattern.matcher(f._1)
      if (matcher.find()) {
        true
      } else {
        false
      }
    }).map(f => {
      val index = f._1.lastIndexOf("/")
      var rowkey = f._1.substring(index + 1)
      val appName = rowkey.substring(0, rowkey.indexOf("."))
      val timestamp = f._2.substring(0, f._2.indexOf(","))
      val fields = f._3
      val time = f._4
      var logtime = 0

      var heapNum = 0f
      if (!timestamp.equals("value")) {
        heapNum = f._2.replace(timestamp + ",", "").toFloat / 1024 / 1024
      } else {
        heapNum = -1
      }
      //todo 这里需要注意文件的第一行数据不是想要的数据
      if (!timestamp.equals("t")) {
        logtime = (timestamp).toInt
      } else {
        logtime = 0
      }
      val js2 = new JSONObject(fields)
      val spider_log_send_ip = js2.getString("spider_log_send_ip")
      (rowkey, spider_log_send_ip, appName, logtime, heapNum)
    }).filter(f => {
      f._4 != 0 && f._5 != -1
    }).toDF("rowkey", "spider_log_send_ip", "application_name", "logtime", "driverHeapNumber")

    val executorHeapDF = qa.filter(f => {
      val index = f._1.lastIndexOf("/")
      var rowkey = f._1.substring(index + 1)
      val pattern = Pattern.compile("application_.*jvm\\.heap\\.used\\.csv")
      val matcher = pattern.matcher(f._1)
      if (matcher.find()) {
        true
      } else {
        false
      }
    }).map(f => {
      val index = f._1.lastIndexOf("/")
      var rowkey = f._1.substring(index + 1)
      val appName = rowkey.substring(0, rowkey.indexOf("."))
      val timestamp = f._2.substring(0, f._2.indexOf(","))
      val fields = f._3
      val time = f._4
      var logtime = 0
      //todo 这里需要注意文件的第一行数据不是想要的数据
      if (!timestamp.equals("t")) {
        logtime = (timestamp).toInt
      } else {
        logtime = 0
      }
      var heapNum = 0f
      if (!timestamp.equals("value")) {
        heapNum = f._2.replace(timestamp + ",", "").toFloat / 1024 / 1024
      } else {
        heapNum = -1
      }
      val js2 = new JSONObject(fields)
      val spider_log_send_ip = js2.getString("spider_log_send_ip")
      //        val time = f
      rowkey = appName + "_" + spider_log_send_ip + "_" + logtime

      (rowkey, f._2, time, logtime, heapNum)
    }).filter(f => {
      f._4 != 0 && f._5 != -1
    }).toDF("rowkey", "message", "time", "logtime", "heapNum")
      .drop("message")
      //      .dropDuplicates("rowkey")
      .selectExpr("CAST(rowkey AS STRING)"
      , "CAST(time AS TIMESTAMP)"
      , "CAST(logtime AS STRING)"
      , "CAST(heapNum AS FLOAT)")
      .withColumn("posttime", to_timestamp($"time", "yyyyMMdd HH:mm:ss"))
      .withWatermark("posttime", "1 minutes")
      .groupBy(window($"posttime", "20 seconds", "20 seconds"), $"rowkey")
      .agg(Map("heapNum" -> "sum"))
      .map(f => {
        val arr = f.getAs[String](1).split("_")
        val id = f.getAs[String](1)
        val appName = arr(0) + "_" + arr(1) + "_" + arr(2)
        val serverIP = arr(3)
        val time = arr(4)
        val heapNumber = f.getAs[Double](2)
        (id, serverIP, appName, time, heapNumber)
      }).toDF("rowkey", "spider_log_send_ip", "application_name", "logtime", "executorHeapNumber")


    val Equ = executorHeapDF.writeStream
      .outputMode("update")
      .foreach(new ExecutorHeapMysqlSink)
      .option("truncate", false)
      .start()

    val Dqu = driverheapDF.writeStream.outputMode("append")
      .foreach(new DriverHeapMysqlSink)
      .option("truncate", false)
      .start()


    Equ.awaitTermination()
    Dqu.awaitTermination()


  }
}
