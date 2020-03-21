package com.sinosoft

import com.sinosoft.LanguegeAnalysis.punc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StructuredStreamingBroadCast {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("StructuredStreamingBroadCast")
      .master("local[*]")
      .getOrCreate()

    val bro = "ok"
    val br = spark.sparkContext.broadcast(bro)

    import spark.implicits._
    val filedf = spark.readStream.textFile("C:\\Users\\zdb\\Desktop\\str")
      .map(f => {
        val value = f.replaceAll(punc, " ").replaceAll("[0-9]+", " ")
        value + br.value
      }).filter(f => f.trim.length > 0)

    val qa = filedf.writeStream.option("truncate", false)
      .outputMode("append")
      .format("console")
      .start()
    qa.awaitTermination()
  }
}
