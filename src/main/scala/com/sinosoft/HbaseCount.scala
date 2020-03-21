package com.sinosoft

import java.text.SimpleDateFormat
import java.util.Date

import com.sinosoft.TemporaryDuty.scalaReadFile
import com.sinosoft.commen.Hbase.OperationHbase
import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.utils.MyUtil
import org.apache.hadoop.hbase.client.{Delete, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object HbaseCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("HbaseCount")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    val path1 = "/zdb/beijing/beijing-keywords.txt"
    val path2 = "/zdb/beijing/beijing-area.txt"
    //    val path1 = "C:/Users/zdb/Desktop/beijing-keywords.txt"
    //    val path2 = "C:/Users/zdb/Desktop/beijing-area.txt"
    val keywordsList = scalaReadFile(spark, path1)
    val areaList = scalaReadFile(spark, path2)
    val map = new mutable.HashMap[String, mutable.HashSet[String]]()
    map.put("keywords", keywordsList)
    map.put("area", areaList)
    val bro = sc.broadcast(map)

    val str = "@大江宁静  2018-12-24 08:23:33　　由于站內的人数过多，消息回复实在是不方便，添加私我v【 MACD5555 】备注【188】，朋有圈第一时间更新策略分析和牛股　　-----------------------------　　@健康人生 2018-12-25 09:24:00　　好的，感谢老师的讲解，已经进入，我已经吸取教训了，投资的路上今后一定跟随老师的脚步！　　-----------------------------　　谢谢老师"
    import scala.util.control._
    // 创建 Breaks 对象
    val loop = new Breaks;
    var result = ""
    // 在 breakable 中循环
    loop.breakable {
      for (reg <- bro.value.get("area").get) {
        if (str.contains(reg)) {
          println(reg)
          loop.break
        }
      }
    }

    println("===================")
    import scala.util.control._
    // 创建 Breaks 对象
    val loop2 = new Breaks;
    // 在 breakable 中循环
    loop2.breakable {
      for (reg <- bro.value.get("keywords").get) {
        if (str.contains(reg)) {
          println(reg)
          loop.break
        }
      }
    }

    sc.stop()
    spark.close()
  }
}
