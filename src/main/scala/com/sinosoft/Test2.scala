package com.sinosoft

import com.sinosoft.ModelBasedCF.Rating
import com.sinosoft.algorithm.BlasSim
import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.javas.HashAl
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.StructType
import org.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source
import scala.util.Random
import scala.util.control.Breaks

object Test2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ScanUsers")
      .master("local[*]")
      .getOrCreate();
    val sc = spark.sparkContext
    val array = Array(2, 4, 6, 67, 3, 45, 26, 35, 789, 345)
    val data = sc.parallelize(array)
    implicit val my_self_Ordering = new Ordering[Int] {
      override def compare(a: Int, b: Int): Int = {
        println("进来了")
        if (a - b > 0) {
          1
        } else {
          -1
        }
      }
    }
    // 替换repartition组合sortBy
    val rdd = data.zipWithIndex()
    rdd.repartitionAndSortWithinPartitions(new HFilePartitioner(2))
      .foreachPartition(f => {
        while (f.hasNext) {
          println(Thread.currentThread().getName + f.next())
        }
      })

    //(3,4)
    //(2,0)
    //(35,7)
    //(4,1)
    //(45,5)
    //(6,2)
    //(67,3)
    //(26,6)
    //(345,9)
    //(789,8)
    spark.close()
  }
}
