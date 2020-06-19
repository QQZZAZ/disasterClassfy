package com.sinosoft

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object OracleTest {
  def main(args: Array[String]): Unit = {
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSqlConOracle")
      .master("local[*]")
      .getOrCreate()
    //设置输出日志级别
    /*val odf = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.23.99:1521:orcl")
      .option("dbtable", "TB_CAMERA_INFO")
      .option("user", "IVMS86X0")
      .option("password", "IVMS86X0")
      .load()
    odf.createOrReplaceTempView("TB_CAMERA_INFO2")
    /*val df = spark.sql("SELECT count(*) from TB_TRAFFIC_VEHICLE_PASS t \n" +
      "where t.PASS_TIME>to_date('2017-07-01 00:00:00','yyyy-mm-dd hh24:mi:ss')\n" +
      "and t.PASS_TIME<to_date('2017-07-31 00:00:00','yyyy-mm-dd hh24:mi:ss')")*/
    val df2 = spark.sql("select EXTRACTIME from TB_CAMERA_INFO2")
    df2.show(false)*/

    val url = "jdbc:oracle:thin:@192.168.23.99:1521:orcl"
    val tableName = "TB_TRAFFIC_VEHICLE_PASS"

    // 设置连接用户&密码
    val prop = new java.util.Properties
    prop.setProperty("user", "IVMS86X0")
    prop.setProperty("password", "IVMS86X0")

    /**
      * 将9月16-12月15三个月的数据取出，按时间分为6个partition
      * 为了减少事例代码，这里的时间都是写死的
      * modified_time 为时间字段
      */

     val predicates =
       Array(
         "2017-07-01 00:00:00" -> "2017-07-02 00:00:00",
        "2017-07-03 00:00:00" -> "2017-07-04 00:00:00",
        "2017-07-05 00:00:00" -> "2017-07-06 00:00:00",
        "2017-07-07 00:00:00" -> "2017-07-08 00:00:00",
        "2017-07-09 00:00:00" -> "2017-07-10 00:00:00",
        "2017-07-11 00:00:00" -> "2017-07-12 00:00:00",
        "2017-07-13 00:00:00" -> "2017-07-14 00:00:00",
        "2017-07-15 00:00:00" -> "2017-07-16 00:00:00",
        "2017-07-17 00:00:00" -> "2017-07-18 00:00:00",
        "2017-07-19 00:00:00" -> "2017-07-20 00:00:00",
        "2017-07-21 00:00:00" -> "2017-07-22 00:00:00"
       ).map {
         case (start, end) =>
           s"PASS_TIME >= to_date('$start','yyyy-mm-dd hh24:mi:ss') "+
             s"AND PASS_TIME < to_date('$end','yyyy-mm-dd hh24:mi:ss')"
       }

   /* val arr = ArrayBuffer[Int]()
    for (i <- 0 to 7) {
      arr.append(i)
    }
    val predicates = arr.map(i => {
      s"mod(LANE_NO,8) = $i"
    }).toArray*/
    val starttime = System.currentTimeMillis()
    val df = spark.read.jdbc(url, tableName, predicates, prop)
    df.write.parquet("D:/traffic1")

    val endtime = System.currentTimeMillis()
    println(endtime - starttime)

    spark.close()
  }
}
