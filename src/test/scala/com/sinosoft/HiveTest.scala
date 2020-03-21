package com.sinosoft
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
object HiveTest {
  def main(args: Array[String]): Unit = {
    /*Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
//            .config("hive.metastore.uris", "thrift://192.168.129.10:9083")
      .master("local[*]")
      /* .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")*/
      .enableHiveSupport()
      .getOrCreate()

    //    spark.sql("use default")
    spark.sql("show databases").show()
    spark.sql("use test")
    spark.sql("show tables").show()

    spark.stop()*/

    val set = new mutable.HashSet[String]()
    set.add("Zdb")
    set.add("Zdb2")

    if(set.contains("zdb")){
      println("ok")
    }

  }
}
