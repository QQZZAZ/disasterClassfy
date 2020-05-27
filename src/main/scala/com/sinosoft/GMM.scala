package com.sinosoft

import java.io.{File, InputStreamReader}

import com.sinosoft.commen.algorithm.{TextRankKeyword, TextRankWordSet}
import org.ansj.library.DicLibrary
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel, KMeans,BisectingKMeansModel}
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import scala.collection.mutable

object GMM {
  @transient
  case class model_instance(features:Vector) extends Serializable

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("GMM")
      .master("local")
      .config("spark.driver.userClassPathFirst",true)
      .getOrCreate()

    val filter = new StopRecognition()
    //仅保留动词，形容词，名词，副词。其他全部过滤
    filter.insertStopNatures("w", "x", "k", "h", "o", "y", "e", "u", "c", "p", "r", "b", "t") //过滤掉标点和空白符
    //添加未登录词
    DicLibrary.insert(DicLibrary.DEFAULT, "版号")
    DicLibrary.insert(DicLibrary.DEFAULT, "日月光华")
    DicLibrary.insert(DicLibrary.DEFAULT, "PC")

    import java.io.BufferedReader
    val strHashMap = new mutable.HashMap[String, String]()
    //获取resources目录下的配置文件
    val fileInputStream = GMM.getClass.getClassLoader.getResourceAsStream("stopWords.txt")
    //读入停用词文件
    val StopWordFileBr = new BufferedReader(new InputStreamReader(fileInputStream))
    var stopWord = StopWordFileBr.readLine
    val keyword = new mutable.HashSet[String]
    while (stopWord != null) {
      keyword += stopWord
      filter.insertStopWords(stopWord)
      stopWord = StopWordFileBr.readLine
    }


    import spark.sqlContext.implicits._
//    val filename = "hdfs:/bigdata/GMM"
    val filename = "D:/data/*"
//    val filename = "hdfs:/test/zdb/file"

    val rddFF = spark.sparkContext.textFile(filename)
      .map(f => {
        val ss = f.replaceAll("[-|\\p{P}|`]", "").replaceAll("[\\s\\p{Zs}]+", " ").replaceAll("\\s+", " ")
        //        println("ss:"+ss)
        var str = if (f.trim.length > 0)
          NlpAnalysis.parse(f.replaceAll("[-|\\p{P}|`]", "").replaceAll("[\\s\\p{Zs}]+", "").replaceAll("\\s+", ""))
            .recognition(filter).toStringWithOutNature(" ")
        str.toString
      }).toDF("sentence").cache()

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(rddFF).cache()
    wordsData.select("words").show(100)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(wordsData).cache()
    featurizedData.select("rawFeatures").foreach(println(_))
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val result = idfModel.transform(featurizedData).cache()

    result.select("features").show(100,false)
    //对数据归一化处理
    val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures").setP(1.0)
    val l1NormData = normalizer.transform(result.select("features")).cache()
    l1NormData.show(100)

    val data = l1NormData.select("features").rdd
      .map(f=>{f.getAs(0).asInstanceOf[Vector].toSparse})
    val df = data.map(f=>{model_instance(Vectors.dense(f.toArray))}).toDF("features").cache()
    val vectorRDD = spark.sparkContext.textFile("D:\\vctor\\*")
    val vector = vectorRDD.map(f=>{
      val arr = f.split(",")
      var number = 0d
      var array = for{
        tt <- arr
        number = tt.toDouble
      }yield number
      //稠密矩阵运算
//      val vector = Vectors.dense(array)
      //稀疏矩阵运算
      val vector = Vectors.sparse(12,Array(0,1,2,3,4,5,6,7,8,9,10,11),array)
      model_instance(vector)
    }).toDF("features")
    vector.select("features").take(10).foreach(println(_))

    println("======================================================")
    val schemaString = "content features"
    spark.udf.register("features",(x:String)=>testArr(x))
    def testArr(code:String)={
      val arr = code.split("\\|\\|")(1).split(",")
      var number = 0d
      var array = for{
        tt <- arr
        number = tt.toDouble
      }yield number
      Vectors.dense(array)
    }

    val vectorRDD2 = spark.sparkContext.textFile("D:\\zdb\\*")
    val vector2 = vectorRDD2.map(f=>{
      val content = f.split("\\|\\|")(0)
      val arr = f.split("\\|\\|")(1).split(",")
      var number = 0d
      var array = for{
        tt <- arr
        number = tt.toDouble
      }yield number
      val vector = Vectors.dense(array)
      (content,vector)
    }).toDF("content","features")
    vector2.select("features").foreach(println(_))

    /*val vector = l1NormData.select("normFeatures").map(f=>{
//      println(Vector.newBuilder(f.getAs(0)))
      val arr = f.get(0).toString.split(",")(2).replaceAll("\\[|\\]","").split(",")
      var number = 0d
      var array = for{
        tt <- arr
        number = tt.toDouble
      }yield number
      val vector = Vectors.dense(array)
      model_instance(vector)
    })*/

    //高斯聚类
    val gm = new GaussianMixture().setK(5).setMaxIter(3).setTol(0.001).setPredictionCol("predict").setProbabilityCol("prob")
    val gmm = gm.fit(vector)
    val result2 = gmm.transform(vector).cache()
    result2.show(100,false)

    //kmeans聚类
    /*val kmeans = new KMeans().setK(5).setSeed(1L).setPredictionCol("home")
    val model = kmeans.fit(l1NormData)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    println("calculating wssse ...")
    val WSSSE = model.computeCost(l1NormData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
    model.transform(l1NormData).select("home").show(100,false)*/

    /*val textRank = new TextRankWordSet
    //对每条文本进行转换
    val newRDD = textRank.transform(rddFF)
//    newRDD.foreach(f=>f.foreach(println(_)))
    val textRankKeyword = new TextRankKeyword
    //对数据进行算法计算
    val rankRDD = textRankKeyword.rank(newRDD)
    //对计算之后的结果进行排序
    val sortRDD = textRankKeyword.sortByValue(rankRDD)

    val rddF = sortRDD.map(f=>f.map(_._1)).toDF("text")



    val word2vec = new Word2Vec().setInputCol("text").setOutputCol("result").setMinCount(1).setVectorSize(100)
      .setNumPartitions(3).setMaxIter(3)
    val model = word2vec.fit(rddF)
    model.write.overwrite().save("D:\\check\\word2vec")
    //对数据归一化处理
   /* val normalizer = new Normalizer().setInputCol("result").setOutputCol("normFeatures").setP(1.0)
    model.transform(rddF).select("result").show(100)
    val l1NormData = normalizer.transform(model.transform(rddF).select("result"))
    l1NormData.show(100)*/
    val resultVector = l1NormData.select("normFeatures").map(f=>{
      val arr = f.get(0).toString.replaceAll("\\[|\\]","").split(",")
      var number = 0d
      var array = for{
        tt <- arr
        number = tt.toDouble
      }yield number
      model_instance(Vectors.dense(array))
    })*/
    /*model.transform(rddF).select("text","result").show(50,false)

    val resultVector2 = model.transform(rddF).select("result").map(f=>{
      val arr = f.get(0).toString.replaceAll("\\[|\\]","").split(",")
      var number = 0d
      var array = for{
        tt <- arr
        number = tt.toDouble
      }yield number
      model_instance(Vectors.dense(array))
    })
*/
    //测试模型
    /*val model2 = Word2VecModel.load("D:\\check\\word2vec")
    val vector = model2.getVectors*/

//    model2.getVectors.filter(f=>f.get(1).toString.toDouble == 0.07115978747606277).show()
//    val synonyms = model2.findSynonyms("计算机", 10).rdd
//    synonyms.foreach(f=>println(f.get(0)+","+f.get(1)))

//    model.write.overwrite().save("D:\\check")

    /*val gm = new GaussianMixture().setK(5).setMaxIter(3).setTol(0.0001).setPredictionCol("predict").setProbabilityCol("prob")
    val gmm = gm.fit(resultVector)
    val result = gmm.transform(resultVector)
    result.show(100,false)
    gmm.write.overwrite().save("D:\\check\\gmm")*/

//    val sameModel = GaussianMixtureModel.load("D:\\check\\gmm")
//    val result  = sameModel.transform(vector)
//    result.show(10,false)


  }

}
