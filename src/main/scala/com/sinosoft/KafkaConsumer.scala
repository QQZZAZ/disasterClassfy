package com.sinosoft

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.sinosoft.sink.{DropDunplicatesHbaseSink, HbaseSink, HotWords2MysqlSink}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.json.JSONObject
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}

import scala.collection.mutable.ArrayBuffer

//Strucrted Streaming 设置checkpoint 防止丢失kafka数据
object KafkaConsumer {
  //  private val zkip = "10.10.30.131:2181,10.10.30.132:2181,10.10.30.133:2181,10.10.30.134:2181,10.10.30.135:2181"
  private val tjzkip = "192.168.129.204:9092,192.168.129.205:9092"
  //  private val tjzkip = "192.168.129.11:2181,192.168.129.12:2181"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("KafkaConsumer").master("local[*]")
      .config("spark.sql.shuffle.partitions", 8)
      //实时业务可以屏蔽日志，以防止日志变得越来越大
      .config("spark.eventLog.enabled",false)
      .getOrCreate()
    import spark.implicits._
    //    System.setProperty("HADOOP_USER_NAME", "root");


    //    spark.streams.addListener()
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", tjzkip)
      .option("subscribe", "sc_kafka_content")
      .option("spark.speculation", "true")
      .option("failOnDataLoss", "false")
      //max.poll.records 和 maxOffsetsPerTrigger 都有用但是设置一个就可以了
      //控制的是kafka得取数据得数量
      .option("max.poll.records", 4800)
      //      .option("maxOffsetsPerTrigger", 96000)
      //从kafka的每个partition最多只取20000条数据，当服务器宕机时使用，因为会积压大量数据
      // 这2个参数是spark streaming的参数无法作用于structred streaming
      //      .option("spark.streaming.kafka.maxRatePerPartition",20000)
      //      .option("spark.streaming.backpressure.enabled",true) //开启内部背压机制与上面的maxRatePerPartition组合使用
      .load()
    import org.apache.spark.sql.functions._
    //    df.show(false)
    val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val jsonOptions = Map("timestampFormat" -> nestTimestampFormat)
    val schema = StructType(List(StructField("thematicId", StringType)
      , StructField("rowkey", StringType)
      , StructField("taskTime", StringType)
      , StructField("keywords", ArrayType(ArrayType(StringType)))
      , StructField("state", StringType)
    ))

    val qa = df.select(from_json(col("value").cast("string"), schema)
      .alias("parsed_value"))
    qa.createOrReplaceTempView("table1")
    val df2 = spark.sql(
      "select " +
        "parsed_value.thematicId," +
        "parsed_value.taskTime," +
        "parsed_value.state," +
        "parsed_value.keywords," +
        "parsed_value.rowkey " +
        "from table1"
    )
    val df3 = df2.filter(f => ((f.getAs[String](0) != null && !f.getAs[String](0).trim.equals("")) &&
      (f.getAs[String](1) != null && !f.getAs[String](1).trim.equals("")) &&
      (f.getAs[String](2) != null && !f.getAs[String](2).trim.equals(""))
      )).map(f => {
      var arr = ""
      val thematicId = f.getAs[String](0)
      val taskTime = f.getAs[String](1)
      val state = f.getAs[String](2)
      val rowkey = f.getAs[String](4)
      //json中的数组需要转换成Seq来解析
      val keyword = f.getAs[Seq[Seq[String]]](3)
      keyword.foreach(f => {
        f.foreach(f => if (arr.length == 0) arr = f else arr = arr + "," + f)
      })
      //      println(arr)
      (thematicId, state, arr, taskTime, rowkey)
    }).toDF("taskId", "state", "keywordsList", "taskTime", "rowkey")

    val qu = df3.writeStream
      //      .option("truncate", false)
      //      .option("checkpointLocation", "/checkpoint")
      .outputMode("append")
      .foreach(new HotWords2MysqlSink)
      //            .format("console")
      /*.trigger(Trigger.ProcessingTime(
        1000
      ))*/ .start()
    qu.awaitTermination()

  }
}