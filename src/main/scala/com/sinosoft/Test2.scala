package com.sinosoft

import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.javas.HashAl
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.ProcessingTime
import org.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random
import scala.util.control.Breaks

object Test2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    import spark.implicits._
    val wordCounts = spark.readStream.text("D:\\data")
      .as[String]
      .map(f => {
        val id = f.split(",")(0)
        val age = f.split(",")(1)
        val rondNum = Random.nextInt(3)
        (id, age, rondNum)
      }).toDF("id", "age", "par")
      .groupByKey(f => f.getAs(2).toString).count()
     /* .flatMapGroups((k, itr) => {
        val arr = new ListBuffer[(String, String)]()
        while (itr.hasNext) {
          val row = itr.next()
          val id = row.getAs[String](0)
          val age = row.getAs[String](1)
          arr.append((id, age))
        }
        arr.iterator
      }).toDF("id","age")*/

    val query = wordCounts.writeStream
      .format("console")
      //      .foreach(new TestForeachWriter())
      .outputMode("complete") //complete  append
      //      .trigger(ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()


  }
}
