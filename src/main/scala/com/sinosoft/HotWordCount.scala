package com.sinosoft

import com.sinosoft.utils.EnumUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object HotWordCount {
  lazy val tjkafkaurl = "192.168.129.205:9092,192.168.129.206:9092,192.168.129.204:9092"
  lazy val xjkafkaurl = EnumUtil.KAFKA_ZOOKEEPER_URL
  lazy private val redisKeyPrefix = "TopicHotWordCount:"
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("TopicHotWordCount")
                  .master("local[*]")
      .getOrCreate()
    val df = spark
      .readStream
      .format("kafka")
      //      .option("kafka.bootstrap.servers", tjkafkaurl)
      .option("kafka.bootstrap.servers", tjkafkaurl)
      .option("subscribe", "sc_kafka_content")
      //从kafka的每个partition最多只取20000条数据，当服务器宕机时使用，因为会积压大量数据
      //      .option("spark.streaming.backpressure.enabled", true) //开启内部背压机制与上面的maxRatePerPartition组合使用
      .option("spark.speculation", true)
      .option("failOnDataLoss", false)
      //max.poll.records 和 maxOffsetsPerTrigger 都有用但是设置一个就可以了
      //控制的是kafka得取数据得数量
      .option("max.poll.records", 4800)
      .load()
    import spark.implicits._
    val schma = StructType(List(StructField("thematicId", StringType)
      , StructField("taskTime", StringType)
      , StructField("keywords", ArrayType(StringType))
      , StructField("state", StringType)
    ))




  }
}
