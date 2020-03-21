package com.sinosoft

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 经验证发现监听失败任务的日志，就是时间无法修正，恐怕会产生海量日志
  * 不知道这个监听器是多长时间调用一次
  * 产生的日志，存在driver客户端所在的服务器上
  * 目前看来不如爬虫爬取sparkUI界面来的简单
  */
object StructuredStreamingWindowTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
//      .master("local[*]")
      .appName("WindowTest")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.addSparkListener(new MyListener)
    val rdd = sc.parallelize(List("1","a","b","c","d","e"))

    rdd.map(f=> f.toString.toInt).foreach(f=>{
      Logger.getRootLogger.error("ok"+f)
    })

    /*val rankSpec = Window.partitionBy(orders("order_id"))
//    println(rankS)
    val dfn = row_number().over(rankSpec.orderBy("order_id")).alias("rank")


    val shopOrderRank = orders.withColumn("rank",dfn)
    shopOrderRank.show()*/

    spark.close()
  }


}
