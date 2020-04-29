package com.sinosoft

import java.text.SimpleDateFormat
import java.util.Date

import com.sinosoft.commen.Hbase.OperationHbase
import com.sinosoft.commen.mysql.MysqlAPI
import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.utils.{EnumUtil, MyUtil}
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.io.Source
import scala.util.control.Breaks

//专网数据存储位置：
// 微信/zdb/beijing/tmp/wx
//新闻 /zdb/beijing/zwxw

object TemporaryDuty {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("TemporaryDuty")
      //      .config("redis.host", "192.168.129.211")
      //      .config("redis.port", "6380")
      //      .config("redis.timeout", "100000")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    val kwlist = sc.textFile("C:\\Users\\zdb\\Desktop\\keywords.txt").collect()

    val bro = sc.broadcast(kwlist)
    val df = spark.read.textFile("C:\\Users\\zdb\\Desktop\\s_data_pipei_2020_03_06\\s_data_pipei_2020_03_06")
    val df2 = df.map(f => {
      val keyList = bro.value
      val content = f
      var key = ""
      val loop = new Breaks
      loop.breakable {
        for (keys <- keyList) {
          if (content.contains(keys)) {
            key = keys
            loop.break()
          }
        }
      }
      (key, 1)
    }
    ).rdd.reduceByKey(_ + _).cache()


    /*val df3 = spark.read.textFile("C:\\Users\\zdb\\Desktop\\s2")
    val df4 = df3.map(f => {
      val keyList = bro.value
      val content = f
      var key = ""
      val loop = new Breaks
      loop.breakable {
        for (keys <- keyList) {
          if (content.contains(keys)) {
            key = keys
            loop.break()
          }
        }
      }
      (key, 1)
    }
    ).rdd.reduceByKey(_ + _).cache()
    val result = df2.union(df4).reduceByKey(_+_).cache()
    result.collect().foreach(println(_))
    val num = result.map(_._2).reduce(_+_)
    println(num)*/
    df2.foreach(println(_))
    println(df2.map(_._2).reduce(_+_)+"条数据")
    spark.close()
    //    sc.stop()

  }

  def readFile(spark: SparkSession, path: String, num: Int): Unit = {
    import spark.implicits._
    val df = spark.read.textFile(path)
      .map(f => {
        val content = f.toString.split("\\#\\#\\$\\$\\%\\%\\@\\@\\#\\#")
        var data = ""
        try {
          if (content.size == 2 && content(1).size >= num) {
            data = content(1).substring(0, num)
          } else {
            data = content(1).substring(0, content(1).size)
          }
        } catch {
          case e: Exception => println(f)
        }

        data
      }).map(f => (f, 1))
    val map = df.rdd.countByKey()
    val list = map.toSeq.sortWith(_._2 > _._2).take(100)
    spark.createDataset(list).coalesce(1).write.mode(SaveMode.Append).csv("D:/data/result")
  }

  def scalaReadFile(spark: SparkSession, path: String) = {
    val file = spark.read.textFile(path)
    val set = new mutable.HashSet[String]()
    val list = file.collect().iterator
    while (list.hasNext) {
      val value = list.next()
      set.add(value)
    }
    set
  }
}
