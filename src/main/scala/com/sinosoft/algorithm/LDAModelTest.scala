package com.sinosoft.algorithm

import java.io.PrintWriter

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.sinosoft.algorithm.TF_IDF_Model.{getDF, transform}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.feature.{HashingTF => MLIBHashingTF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LDAModelTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("LDAModelTest")
      .getOrCreate()


    val list = List(
      "男子住进凶宅后家人相继去世,死因离奇",
      "重磅！中国人的假期或将大调整",
      "谢娜被开除张杰痛骂湖南台？实情曝光",
      "她曾为追刘德华致父死 如今生活成这样",
      "大连10岁女童仍未火化 遗像挂凶手门口"
    )
    val sc = spark.sparkContext
    import spark.implicits._
    val abb = new mutable.HashSet[String]()
    val bro = sc.broadcast(abb)
    //    val datardd = sc.parallelize(list)

    val abb2 = new mutable.HashSet[String]()
    val bro2 = sc.broadcast(abb2)

    val datardd = sc.wholeTextFiles("D:\\data\\caijing")
    val datardd1 = sc.wholeTextFiles("D:\\data\\caipiao")
    val datardd2 = sc.wholeTextFiles("D:\\data\\fangchan")
    val datardd3 = sc.wholeTextFiles("D:\\data\\gupiao")
    val datardd4 = sc.wholeTextFiles("D:\\data\\jiaju")
    val datardd5 = sc.wholeTextFiles("D:\\data\\jiaoyu")

    val rddAll = datardd
      .union(datardd1)
      .union(datardd2)
      .union(datardd3)
      .union(datardd4)
      .union(datardd5)

    val dff: DataFrame = getDF(datardd, spark, bro, bro2, 0)
    val dff1: DataFrame = getDF(datardd1, spark, bro, bro2, 1)
    val dff2: DataFrame = getDF(datardd2, spark, bro, bro2, 2)
    val dff3: DataFrame = getDF(datardd3, spark, bro, bro2, 3)
    val dff4: DataFrame = getDF(datardd4, spark, bro, bro2, 4)
    val dff5: DataFrame = getDF(datardd5, spark, bro, bro2, 5)

    val dffAll = dff
      .union(dff1).union(dff2).union(dff3).union(dff4).union(dff5)

    dffAll.count()
    println("bro.value.size:" + bro.value.size)
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("sentence")
      .setOutputCol("features")
      .setMinDF(3) //控制词再不同文档中出现的次数总值的最小值
      .setVocabSize(bro.value.size)
      .setMinTF(2) //在控制词出现在本篇文章出现的最少次数
      .fit(dffAll)

    val featureDF = cvModel.transform(dffAll)
      .select("features", "index", "label")

    val lda = new LDA()
      .setK(6)
      .setFeaturesCol("features")
      .setTopicConcentration(3)
      .setDocConcentration(3)
      //"em"占内存 online 不占
      .setOptimizer("online")
      .setCheckpointInterval(10)
      .setMaxIter(100)

    val model = lda.fit(featureDF)

    val topicsProb = model.transform(featureDF)
    val featuDF = topicsProb.select("label", "index", "topicDistribution")

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("topicDistribution")
      //entropy 熵 gini 基尼系数
      .setImpurity("entropy") // Gini不纯度，entropy熵
      .setMaxBins(6) // 离散化"连续特征"的最大划分数
      .setMaxDepth(20) // 树的最大深度
      .setMinInfoGain(0.2) //一个节点分裂的最小信息增益，值为[0,1]
      .setMinInstancesPerNode(10) //每个节点包含的最小样本数
      .setProbabilityCol("prob")

    val dtModel = dt.fit(featuDF)
    val result = dtModel.transform(featuDF)
    result.select("label", "prob", "prediction").show(2000, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(result)
    println(accuracy)

    spark.close()
  }

  private def getDF(datardd: RDD[(String, String)],
                    sparkSession: SparkSession,
                    bro: Broadcast[mutable.HashSet[String]],
                    bro2: Broadcast[mutable.HashSet[String]],
                    label: Int) = {
    import sparkSession.implicits._
    val dff = datardd.map(text => {
      val sb = new ArrayBuffer[String]()
      val wordsList = transform(text._2, bro)
      for (str <- wordsList) {
        if (str.word.length > 1) {
          sb.append(str.word)
          bro.value.+=(str.word)
        }
      }
      (label, text._1, sb.toArray)
    }).toDF("label", "index", "sentence")
    dff
  }

  // 结果转换，可以不显示词性
  def transform(sentense: String, bro: Broadcast[mutable.HashSet[String]]) = {
    import com.hankcs.hanlp.seg.NShort.NShortSegment
    import com.hankcs.hanlp.seg.Segment
    val nShortSegment = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true)
    val list2 = nShortSegment.seg(sentense)
    CoreStopWordDictionary.apply(list2)
    val set = bro.value
    import scala.collection.JavaConverters._
    val sList = list2.asScala
    //    val list = sList.map(x => x.word.replaceAll(" ", "")).toList
    sList
  }

}
