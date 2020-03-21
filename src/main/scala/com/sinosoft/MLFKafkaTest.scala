package com.sinosoft

import com.sinosoft.sink.HotWords2MysqlSink
import com.sinosoft.utils.EnumUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject
import spark.jobserver.CommonMessages.Subscribe

/**
  * Created by SparkUser on 2018/7/10.
  */
object MLFKafkaTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("kafkaTest")
//      .master("local[*]")
      .getOrCreate()
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.93.205.84:9092,10.93.205.85:9092," +
        "10.93.205.86:9092,10.93.205.87:9092,10.93.205.88:9092,10.93.205.89:9092," +
        "10.93.205.90:9092,10.93.205.91:9092,10.93.205.92:9092,10.93.205.93:9092")
      .option("subscribe", "sc_kafka_content")
//      .option("kafka.bootstrap.servers","192.168.129.204:9092,192.168.129.205:9092")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("max.poll.size",4800)
      .load()

//    df.printSchema()
    import spark.implicits._
    val qa = df.select("value")
      .selectExpr("CAST(value AS STRING)")
      .as[String].map(f => {
      val js = new JSONObject(f)
      val rowkey = js.getString("infoRowkey")
      val tableName = js.getString("infoTableName")
      (rowkey,tableName)
    }).repartition(48).map(f=>{
      val rowkey = f._1
      val tableName = f._2
      if(rowkey.trim.equals("1081649703-IkLXD66Gk")){
        rowkey
      }else{
        ""
      }
    })
    qa.filter(!_.equals("")).coalesce(1).write.mode(SaveMode.Append)
//      .text("D:/data/3")
      .text("/zdb/data")
  }

}
