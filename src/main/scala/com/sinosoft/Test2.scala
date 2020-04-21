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
import scala.util.Random
import scala.util.control.Breaks

object Test2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    println("1")
    //数据结构 用户，物品，评分
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
    println("2")

    val rdd = spark.createDataset(ListAll).rdd.repartition(8)
    println("3")

    val urdd = spark.createDataset(ListAll).rdd.repartition(8)
      .map(_._1)
      .distinct().zipWithIndex()
      .map(f => (f._1, f._2.toInt))
      .cache()
    println("4")

    val irdd = spark.createDataset(ListAll).rdd.repartition(8)
      .map(_._2._1)
      .distinct().zipWithIndex()
      .map(f => (f._1, f._2.toInt))
      .cache()
    println("5")

    val FJ = rdd.join(urdd).cache()

    val SJ = FJ.map(x => (x._2._1._1, (x._2._2, x._2._1._2))).join(irdd)
      .map(x => Rating(x._2._1._1.toInt, x._2._2.toInt, x._2._1._2.toDouble)).toDF()
      .cache()
    println("6")

    val splits = SJ.randomSplit(Array(0.8, 0.2)); // //对数据进行分割，80%为训练样例，剩下的为测试样例。
    splits(0).show(false)
    splits(1).show(false)

    println("====================================")
    val training = splits(0)
    val test = splits(1)

    val als = new ALS()
      .setMaxIter(10)
      .setRank(15) //rank是模型中潜在因素的数量（默认为10）最终影响矩阵特征值的维度数量
      .setNumBlocks(8) //分块数：分块是为了并行计算，默认为10。
      .setRegParam(0.1) //正则化数据
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      //如果score是隐式的维度，例如浏览和收藏 而评分则属于显示的维度如果计算隐式则设置为true
      //如果计算的是显示维度则用默认值即可
      .setImplicitPrefs(true)
      .setAlpha(1.0)

    val model = als.fit(training)
    val itemFeature = model.itemFactors.rdd.map(
      row => {
        val item = row.getInt(0)
        val vec = row.get(1).asInstanceOf[mutable.WrappedArray[Float]]
        (item, vec)
      }
    ).sortByKey().collect()


    //物品的数量
    val numItems = irdd.count().toInt
    //物品的特征向量
    val itemVectors = itemFeature.flatMap(x => x._2)
    //物品id，物品索引
    val itemIndex_tmp = irdd.collectAsMap()

    val blasSim = new BlasSim(numItems, 15, itemVectors, itemIndex_tmp)
    val itemString = irdd.map(x => (x._2, x._1)).repartition(8)

    val item_sim_rdd = itemString.map(x => {
      //物品id，
      (x._2, blasSim.getCosinSimilarity((itemFeature(x._1)._2.toVector), 50, None).toList)
    })

    item_sim_rdd.foreach(println(_))

    spark.close()

  }
}
