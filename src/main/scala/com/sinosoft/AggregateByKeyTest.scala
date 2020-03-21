package com.sinosoft

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object AggregateByKeyTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
    //准备数据
    val pairRdd = spark.sparkContext.parallelize(
      List(
        ("84.174.205.5", ("2018-11-10 23:23:23", 2)),
        ("221.226.113.146", ("2018-09-11 23:23:23", 3)),
        ("84.174.205.5", ("2018-12-15 23:23:23", 5)),
        ("108.198.168.20", ("2018-01-03 23:23:23", 2)),
        ("108.198.168.20", ("2018-11-21 23:23:23", 4)),
        ("221.226.113.146", ("2018-11-01 23:23:23", 6)),
        ("221.226.113.146", ("2018-12-06 23:23:23", 6))
      ), 2)
    //运用aggregatebykey
    //1、U定义为ArrayBuffer
    val juhe = pairRdd.aggregateByKey(scala.collection.mutable.ArrayBuffer[(String, Int)]())((arr, value) => {
      //2、将value放入集合U中
      arr += value
      //3、将所有的集合进行合并
    }, _.union(_))
    println("=====================")
    juhe.foreach(println(_))
    println("=====================")


    val juhesum = juhe.mapPartitions(partition => {
      partition.map(m => {
        val key = m._1
        val date = m._2.map(m => m._1).toList.sortWith(_ < _)(0)
        val sum = m._2.map(m => m._2).sum
        Row(key, date, sum)
      })
    })
    juhesum.foreach(println(_))

    //替换groupByKey ，聚合取最大值
    val data = List(("a",3l),("a",2l),("a",4l),("b",3l),("c",6l),("c",8l))
    val sc = spark.sparkContext
    val rdd = sc.parallelize(data)

    val res : RDD[(String,Long)] = rdd.repartition(4).aggregateByKey(0l)(
      // seqOp task聚合
      math.max(_,_),
      // combOp executor聚合
      math.max(_,_)
    )

    res.collect.foreach(println)

  }

  //scala 读取文件，并实现generrator
  def scalaReadFile(): Unit = {
    val file = Source.fromFile("C:\\Users\\zdb\\Desktop\\uerUrl.csv")
    var str = ""
    val arr = for {
      line <- file.getLines
    } yield line

    while (arr.hasNext) {
      val ss = arr.next()
      Thread.sleep(500)
      println(ss)
    }
    file.close
  }
}
