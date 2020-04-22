package com.sinosoft

import java.util

import com.sinosoft.utils.{ExcelReaderUtil, MyUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.codehaus.janino.Java

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object KongGangCarCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Test")
      .config("spark.testing.memory", "750000000")
      //      .config("spark.driver.memory","4g")
      //      .config("spark.executor.memory","8g")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    service("D:/data1", sc, spark)


    spark.close()

  }


  def service(path: String, sc: SparkContext, sparkSession: SparkSession): Unit = {
    import scala.collection.JavaConversions._
    import sparkSession.implicits._
    val arr = new util.ArrayList[String]()
    ExcelReaderUtil.getAllFileName(path, arr)

    val list = arr.toBuffer
    //    println(list.size)
    //    list.foreach(println(_))
    val arrayBuffer = new util.ArrayList[util.ArrayList[String]]()
    for (filePath <- list) {
      val javaList = ExcelReaderUtil.readExcel("D:\\data1\\" + filePath)
      arrayBuffer.addAll(javaList)
    }
    arr.clear()
    list.clear()
    val dataList = ExcelReaderUtil.getMaps(arrayBuffer).toList
    println(dataList.size)
    arrayBuffer.clear()
    val rdd = sc.parallelize(dataList).cache()
    val df = rdd.map(f => {
      val carId = f.split("\\|\\|\\|")(0)
      //      val time = f.split("\\|\\|\\|")(1)
      val time = MyUtils.timeTransformation(f.split("\\|\\|\\|")(1))
      (carId, time)
    }).filter(f => !f._1.trim.equals("车牌") && !f._1.trim.equals("车牌号码"))
      .aggregateByKey(scala.collection.mutable.Set[Long]())((arr, value) => {
        //2、将value放入集合U中
        arr += value
        //3、将所有的集合进行合并
      }, _.union(_))
      .map(f => {
        val carid = f._1
        val set = f._2.toSeq.sorted.toList
        var diff = 0l
        var i = 0
        val loop = new Breaks
        loop.breakable(
          for (data <- set) {
            if (i + 1 >= set.size) {
              loop.break()
            }
            val Fdiff = set(i + 1) - set(i)
            if (diff < Fdiff) {
              diff = Fdiff
            }
            i += 2
          }
        )
        diff = diff / 1000 / 60
        (carid, diff)
      })
    val count = df.count()
    println("总共计算车辆：" + count)
    /*df.filter(f => f._2 != 0)
      .foreach(f => {
        println("carid：" + f._1 + " 驻留时长" + f._2 + "分钟")
      })*/

    df.map(f => {
      var stage = ""
      if (f._2 <= 30) {
        stage = "A"
      } else if (f._2 > 30 && f._2 <= 60) {
        stage = "B"
      } else if (f._2 > 60) {
        stage = "C"
      }
      (stage, 1)
    }).reduceByKey(_ + _).foreach(println(_))

    df.toDF("carId","time")
      .write.csv("d:/result")
  }


}
