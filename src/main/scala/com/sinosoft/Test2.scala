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
    data.map(f=>f % 8).foreach(println(_))
    spark.close()
  }
}
