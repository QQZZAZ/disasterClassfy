package com.sinosoft

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MainClass {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ScanUsers")
      .master("local[12]")
//      .config("spark.sql.parquet.compression.codec","lzo")
      .getOrCreate();
    val sc = spark.sparkContext
    val array = Array(2, 4, 6, 67, 3, 45, 26, 35, 789, 345)
//    val data = sc.textFile("D:/test")
//    data.count()
//    import spark.implicits._
//    data.toDF("num")
//      .repartition(1)
//      .write
//      .parquet("D:/lzo1")

    val data = spark.read.parquet("D:/lzo1")
    data.filter(f=>{f.getAs(0).toString.contains("股票")}).count()

//    val data2 = spark.read.text("D:/1.txt")
//    data2.filter(f=>{f.getAs(0).toString.contains("1")}).count()



    spark.close()
    sc.stop()
  }
}
