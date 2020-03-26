package com.sinosoft

import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.javas.HashAl
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

import scala.collection.mutable
import scala.util.control.Breaks

object Test2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("test2")
      //      .config("redis.host", "192.168.129.211")
      //      .config("redis.port", "6380")
      //      .config("redis.timeout", "100000")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
//    val list = spark.read.textFile("C:/Users/zdb/Desktop/new_wx_dt.txt").collect()

    val array=Array(2,4,6,67,3,45,26,35,789,345)
    val data=sc.parallelize(array)
    // 替换repartition组合sortBy
    data.zipWithIndex()
      .repartitionAndSortWithinPartitions(new HashPartitioner(1)).foreach(println)

    spark.close()
    sc.stop()
  }
}
