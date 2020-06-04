package com.sinosoft.algorithm

import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.io.File
import java.util

import org.apache.spark.mllib.util.MLUtils

import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.summary.TextRankKeyword
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import com.sinosoft.utils.ExcelReaderUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LinearSVC, NaiveBayes}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.io.{BufferedSource, Source}


/**
  * 生成词向量的TF模型和IDF模型
  */
object TF_IDF_Model {
  /**
    * 计算每个文档的tf值
    *
    * @param wordAll
    * @return Map<String,Float> key是单词 value是tf值
    */
  def tfCalculate(wordAll: String) = {
    //存放单词，单词数量
    val dictMap = Map[String, Int]()

    var wordCount = 1

    /**
      * 统计每个单词的数量，并存放到map中去
      * 便于以后计算每个单词的词频
      * 单词的tf=该单词出现的数量n/总的单词数wordCount
      */
    for (word <- wordAll.split(" ")) {
      wordCount += 1
      if (dictMap.contains(word)) {
        dictMap.put(word, dictMap(word) + 1);
      } else {
        dictMap.put(word, 1);
      }
    }
    //存放单词，单词频率
    val tfMap = dictMap.map(f => {
      val wordTf = f._2.toFloat / wordCount
      (f._1, wordTf)
    })
    tfMap

  }

  /**
    *
    * @param D         总文档数
    * @param doc_words 每个文档对应的分词
    * @param tfMap     计算好的tf,用这个作为基础计算tfidf
    * @return 每个文档中的单词的tfidf的值
    */
  def idfCalculate(D: Int, doc_word: Map[String, String], tfMap: Map[String, Float]) = {
    val idfMap = Map[String, Float]()
    val wordsList = ArrayBuffer[String]()
    //tfMap中的词频的值没有用上，这里的逻辑有点问题
    for (tfKey <- tfMap.keySet) {
      var Dt = 0
      for (doc <- doc_word) {
        val words = doc._2.split(" ")
        for (word <- words) {
          wordsList.append(word)
        }
        if (wordsList.contains(tfKey)) Dt = Dt + 1
      }
      //总文档数 / 在所有文档中出现某个词的次数
      val idfvalue = Math.log(D.toFloat / Dt).toFloat
      idfMap.put(tfKey, idfvalue)
    }

    idfMap
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("test")
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
    val set = new mutable.HashSet[String]()
    val bro = sc.broadcast(set)
    //    val datardd = sc.parallelize(list)

    val datardd = sc.textFile("D:\\data1\\caijing").repartition(8)
//    val datardd1 = sc.textFile("D:\\data1\\caipiao").repartition(8)
    val datardd2 = sc.textFile("D:\\data1\\fangchan").repartition(8)
//    val datardd3 = sc.textFile("D:\\data1\\gupiao").repartition(8)
    val datardd4 = sc.textFile("D:\\data1\\jiaju").repartition(8)
    val datardd5 = sc.textFile("D:\\data1\\jiaoyu").repartition(8)

    val dff: DataFrame = getDF(bro, datardd, spark, 0)
//    val dff1: DataFrame = getDF(bro, datardd1, spark, 1)
    val dff2: DataFrame = getDF(bro, datardd2, spark, 2)
//    val dff3: DataFrame = getDF(bro, datardd3, spark, 3)
    val dff4: DataFrame = getDF(bro, datardd4, spark, 4)
    val dff5: DataFrame = getDF(bro, datardd5, spark, 5)
    val dffAll = dff
//      .union(dff1)
      .union(dff2)
//      .union(dff3)
      .union(dff4)
      .union(dff5)

    //    dff.show(false)
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

    val wordDF = tokenizer.transform(dffAll)
    dffAll.count()
    println("bro.value.size " + bro.value.size)
    val tf = new HashingTF().setInputCol("words").setOutputCol("tfNum").setNumFeatures(bro.value.size)
    val featureDF = tf.transform(wordDF)
    val idf = new IDF().setInputCol("tfNum").setOutputCol("features")

    val idfmodel = idf.fit(featureDF)
    val rescaledData = idfmodel.transform(featureDF).select("label", "features")
    //      .rdd.map(f => {
    //      val vctor = f.getAs(0).asInstanceOf[org.apache.spark.ml.linalg.SparseVector].toDense
    //将ml的vector转换为mllib的vector
    //      org.apache.spark.mllib.linalg.Vectors.fromML(vctor)
    //    })

    /*val denseVector = r.getAs[org.apache.spark.ml.linalg.SparseVector]("features").toDense
    org.apache.spark.mllib.linalg.Vectors.fromML(denseVector)*/

    /*//稀疏矩阵转换为稠密矩阵
    val mlSv = new MLSparseVector(5, Array[Int](0, 3), Array[Double](1, 2))
    println(mlSv) //(5,[0,3],[1.0,2.0])
    println(mlSv.toDense) //[1.0,0.0,0.0,2.0,0.0]*/

    /**
      * 一、数据标准化
      * StandardScaler (基于特征矩阵的列，将属性值转换至服从正态分布)
      * 标准化是依照特征矩阵的列处理数据，其通过求z-score的方法，将样本的特征值转换到同一量纲下
      * 常用与基于正态分布的算法，比如回归
      *
      * 二、数据归一化
      * MinMaxScaler （区间缩放，基于最大最小值，将数据转换到0,1区间上的）
      * 提升模型收敛速度，提升模型精度
      * 常见用于神经网络
      *
      * 三、Normalizer （基于矩阵的行，将样本向量转换为单位向量）
      * 其目的在于样本向量在点乘运算或其他核函数计算相似性时，拥有统一的标准
      * 常见用于文本分类和聚类、logistic回归中也会使用，有效防止过拟合
      */
    //    val normalizer = new Normalizer()

    //    val l1NormData = normalizer.transform(rescaledData)

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0) // L1范式正则化向量 ，默认是L2范式




    val l1NormData = normalizer.transform(rescaledData).repartition(100)
    println("===============================")
    l1NormData.show(false)
    println("===============================")
    // 创建PCA模型，生成Transformer 降维
    val pca = new PCA()
      .setInputCol("normFeatures")
      .setOutputCol("pcaFeatures")
      .setK(4)
      .fit(l1NormData)

    //  transform数据，生成主成分特征
    val pcaResult = pca.transform(l1NormData).select("label", "pcaFeatures").toDF("label", "features")
    pcaResult.show(false)
    // 将经过主成分分析的数据，按比例划分为训练数据和测试数据
    val Array(trainingData, testData) = pcaResult.randomSplit(Array(0.7, 0.3))
    // 创建SVC分类器(Estimator)
    val lsvc = new LinearSVC()
      .setMaxIter(20)
      .setRegParam(0.01)

    // SVM二元分类训练分类器，生成模型(Transformer)
    /*val lsvcModel = lsvc.fit(trainingData)
    // 用训练好的模型，验证测试数据
    val res = lsvcModel.transform(testData).select("prediction", "label")

    // 计算精度
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(res)

    println(s"Accuracy = ${accuracy}")

    // 将标签与主成分合成为一列
    val assembler = new VectorAssembler()
      .setInputCols(Array("label", "features"))
      .setOutputCol("assemble")
    val output = assembler.transform(pcaResult)

    // 输出csv格式的标签和主成分，便于可视化
    val ass = output.select(output("assemble").cast("string"))
    ass.write.mode("overwrite").csv("D:/output.csv")
*/

    //决策树多元分类 0.8226594351073709
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      //entropy 熵 gini 基尼系数
      .setImpurity("entropy") // Gini不纯度，entropy熵
      .setMaxBins(6) // 离散化"连续特征"的最大划分数
      .setMaxDepth(20) // 树的最大深度
      .setMinInfoGain(0.2) //一个节点分裂的最小信息增益，值为[0,1]
      .setMinInstancesPerNode(10) //每个节点包含的最小样本数
      .setProbabilityCol("prob")

    //      .setSeed(123456)
    //    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(trainingData)

    val KMeansdata = new KMeans()
      .setK(4)
      .setTol(0.001)
      .setSeed(100)
      .setPredictionCol("prediction")
      .setInitMode("random")
      .setInitSteps(2)
      .setMaxIter(2000)
      .setFeaturesCol("features")


    val labelConverter = new IndexToString().
      setInputCol("prediction").
      setOutputCol("predictedLabel").
      setLabels(Array[String]("0", "1", "2", "3", "4", "5"))

    /*val pipeline = new Pipeline().
      setStages(Array(dt, labelConverter))
    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)
    predictions.
      select("predictedLabel", "label", "prob").show(2000, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(accuracy)*/

    val kmodel = KMeansdata.fit(trainingData)
    val predictions = kmodel.transform(pcaResult).select("label","prediction")

    predictions.groupBy("label","prediction")
      .count()
      .orderBy("label","prediction")
      .show(2000,false)


    println("Cluster centers:")
    for (c <- kmodel.clusterCenters) {
      println("  " + c.toString)
    }

    val wsse = kmodel.computeCost(trainingData)
    println(wsse)

    spark.close()
  }


  private def getDF(bro: Broadcast[mutable.HashSet[String]], datardd: RDD[String], sparkSession: SparkSession, num: Int) = {
    import sparkSession.implicits._
    val dff = datardd.map(text => {
      val sb = new mutable.StringBuilder()
      val wordsList = transform(text, bro)
      for (str <- wordsList) {
        sb.append(str)
        sb.append(" ")
      }
      (num, sb.toString())
    }).toDF("label", "sentence")
    dff
  }

  // 结果转换，可以不显示词性
  def transform(sentense: String, set: Broadcast[mutable.HashSet[String]]) = {
    val list2 = HanLP.extractKeyword(sentense, 35)
    //    CoreStopWordDictionary.apply(list2)
    import scala.collection.JavaConverters._
    val sList = list2.asScala
    //    val list = sList.map(x => x.word.replaceAll(" ", "")).toList
    sList.foreach(f => set.value.+=(f))
    sList
  }

  //将同以文本的多行数据拉平成一行
  def readAndWriteFile(path: String): Unit = {
    import scala.collection.JavaConverters._
    val fileNameList = new util.ArrayList[String]()
    ExcelReaderUtil.getAllFileName(path, fileNameList)
    var file: BufferedSource = null
    var writer: PrintWriter = null
    val fileList = fileNameList.asScala.iterator
    while (fileList.hasNext) {
      val path2 = fileList.next()
      file = Source.fromFile(path2)
      var i = 1
      var sb = new mutable.StringBuilder()
      for (line <- file.getLines) {
        sb.append(line)
      }

      val file2 = new File(path2.replace("data", "data1"))
      writer = new PrintWriter(file2)
      writer.write(sb.toString())
      writer.flush()
    }

    file.close
    writer.close()
  }
}
