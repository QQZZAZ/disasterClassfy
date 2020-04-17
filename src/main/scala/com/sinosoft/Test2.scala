package com.sinosoft

import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.javas.HashAl
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.StructType
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
    val sc = spark.sparkContext

    val ListAll = Seq(
      ("a", ("a1", 3.0)),
      ("a", ("b1", 1.0)),
      ("a", ("c1", 5.0)),
      ("b", ("f1", 3.0)),
      ("c", ("f1", 3.0)),
      ("d", ("f1", 1.0)),
      ("e", ("e1", 5.0)),
      ("f", ("e1", 4.0)),
      ("f", ("a1", 3.0)),
      ("c", ("e1", 2.0)),
      ("d", ("e1", 1.0)),
      ("e", ("b1", 1.0)),
      ("e", ("a1", 3.0)),
      ("a", ("f1", 4.0)),
      ("a", ("e1", 5.0))
    )

    val rdd = spark.createDataset(ListAll).rdd.repartition(3)

    val urdd = spark.createDataset(ListAll).rdd.repartition(3)
      .map(_._1)
      .distinct().zipWithIndex()
      .cache()
    urdd.foreach(println(_))
    println("++++++++++++++++++++++++++++++")

    val irdd = spark.createDataset(ListAll).rdd.repartition(3)
      .map(_._2._1)
      .distinct().zipWithIndex()
      .cache()
    irdd.foreach(println(_))
    println("++++++++++++++++++++++++++++++")


    val FJ = rdd.join(urdd).cache()
    FJ.foreach(println(_))
    println("============================")
    val SJ = FJ.map(x => (x._2._1._1, (x._2._2, x._2._1._2))).join(irdd).cache()
    SJ.foreach(println(_))

    spark.close()

  }
}
