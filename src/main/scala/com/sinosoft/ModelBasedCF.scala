package com.sinosoft

import com.sinosoft.algorithm.BlasSim
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * 百万级物品的协同过滤推荐系统
  */
object ModelBasedCF {

  case class Rating(userId: Int, movieId: Int, rating: Double)

  /** 基于dt时间获取原始数据源
    *
    * @param spark SparkContext
    * @param hql   hql
    * @return 原始数据的dataFrame
    */
  def getResource(spark: SparkSession, hql: String) = {
    import spark.sql
    val resource = sql(hql)
    resource
  }

  /**
    * 基于item相似度矩阵为user生成topN推荐列表
    *
    * @param resource
    * @param item_sim_bd
    * @param topN
    * @return RDD[(user,List[(item,score)])]
    */
  def recommend(resource: DataFrame, item_sim_bd: Broadcast[scala.collection.Map[String, List[(String, Double)]]], topN: Int = 50) = {
    val user_item_score = resource.rdd.map(
      row => {
        val uid = row.getString(0)
        val aid = row.getString(1)
        val score = row.getDouble(2)
        ((uid, aid), score)
      }
    )
    /*
     * 提取user_item_score为((user,item2),sim * score)
     * RDD[(user,item2),sim * score]
     */
    val user_item_simscore = user_item_score.flatMap(
      f => {
        val items_sim = item_sim_bd.value.getOrElse(f._1._2, List(("0", 0.0)))
        for (w <- items_sim) yield ((f._1._1, w._1), w._2 * f._2)
      })

    /*
     * 聚合user_item_simscore为 (（user,item2),sim1 * score1 + sim2 * score2）)
     * 假设user观看过两个item,评分分别为score1和score2，item2是与user观看过的两个item相似的item,相似度分别为sim1，sim2
     * RDD[(user,item2),sim1 * score1 + sim2 * score2）)]
     */
    val user_item_rank = user_item_simscore.reduceByKey(_ + _, 1000)
    /*
     * 过滤用户已看过的item,并对user_item_rank基于user聚合
     * RDD[(user,CompactBuffer((item2,rank2）,(item3,rank3)...))]
     */
    // val user_items_ranks = user_item_rank.subtractByKey(user_item_score).map(f => (f._1._1, (f._1._2, f._2))).groupByKey(500)
    val user_items_ranks = user_item_rank.map(f => (f._1._1, (f._1._2, f._2))).groupByKey(500)
    /*
     * 对user_items_ranks基于rank降序排序，并提取topN,其中包括用户已观看过的item
     * RDD[(user,ArrayBuffer((item2,rank2）,...,(itemN,rankN)))]
     */
    val user_items_ranks_desc = user_items_ranks.map(f => {
      val item_rank_list = f._2.toList
      val item_rank_desc = item_rank_list.sortWith((x, y) => x._2 > y._2)
      (f._1, item_rank_desc.take(topN))
    })
    user_items_ranks_desc
  }

  /**
    * 计算推荐的召回率
    *
    * @param recTopN
    * @param testData
    */
  def getRecall(recTopN: RDD[(String, List[(String, Double)])], testData: DataFrame) = {
    val uid_rec = recTopN.flatMap(r => {
      val uid = r._1
      val itemList = r._2
      for (item <- itemList) yield (uid, item._1)
    })
    val uid_test = testData.rdd.map(row => {
      val uid = row.getString(0)
      val aid = row.getString(1)
      (uid, aid)
    })
    uid_rec.intersection(uid_test).count() / uid_test.count().toDouble
  }

  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .config("spark.default.parallelism", 8)
      .config("spark.sql.shuffle.partitions", 8)
      .appName("ModelBased CF")
      .master("local[*]")
      //      .enableHiveSupport()
      .getOrCreate()

    val trainHql = args(0)
    val testHql = args(1)

    val trainDataFrame = getResource(spark, trainHql).repartition(8).cache()
    val testDataRrame = getResource(spark, testHql).repartition(8)

    val trainRdd = trainDataFrame.rdd.map(row => {
      val uid = row.getString(0)
      val aid = row.getString(1)
      val score = row.getDouble(2)
      (uid, (aid, score))
    }).cache()

    val userIndex = trainRdd.map(x => x._1).distinct().zipWithIndex().map(x => (x._1, x._2.toInt)).cache()
    val itemIndex = trainRdd.map(x => x._2._1).distinct().zipWithIndex().map(x => (x._1, x._2.toInt)).cache()

    import spark.sqlContext.implicits._
    val trainRatings = trainRdd
      .join(userIndex, 8)
      //第一次join之后数据结构：用户ID，（(物品ID，评分)，用户索引）
      .map(x => (x._2._1._1, (x._2._2, x._2._1._2)))
      .join(itemIndex, 8)
      //第二次join之后数据结构：物品ID,（(用户索引，评分)，物品索引）
      .map(x => Rating(x._2._1._1.toInt, x._2._2.toInt, x._2._1._2.toDouble)).toDF()
    //最终数据结果:用户索引，物品索引，评分


    val rank = 200

    /**
      * numBlocks是为了并行化计算而将用户和项目划分到的块的数量（默认为10）。
      * rank是模型中潜在因素的数量（默认为10）。
      * maxIter是要运行的最大迭代次数（默认为10）。
      * regParam指定ALS中的正则化参数（默认为1.0）。
      * implicitPrefs指定是使用显式反馈 ALS变体还是使用 隐式反馈数据（默认为false使用显式反馈的手段）。
      * alpha是一个适用于ALS的隐式反馈变量的参数，该变量管理偏好观察值的 基线置信度（默认值为1.0）。
      * nonnegative指定是否对最小二乘使用非负约束（默认为false）。
      * 注意： ALS的基于DataFrame的API目前仅支持用户和项目ID的整数。用户和项目ID列支持其他数字类型，但ID必须在整数值范围内。
      */
    val als = new ALS()
      .setMaxIter(10)
      .setRank(rank) //rank是模型中潜在因素的数量（默认为10）
      .setNumBlocks(8) //分块数：分块是为了并行计算，默认为10。
      .setRegParam(0.1)  //正则化数据
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      //如果score是隐式的维度，例如浏览和收藏 而评分则属于显示的维度如果计算隐式则设置为true
      //如果计算的是显示维度则用默认值即可
      .setImplicitPrefs(true)
      .setAlpha(1.0)

    val model = als.fit(trainRatings)
    val itemFeature = model.itemFactors.rdd.map(
      row => {
        val item = row.getInt(0)
        val vec = row.get(1).asInstanceOf[mutable.WrappedArray[Float]]
        (item, vec)
      }
    ).sortByKey().collect()

    //物品的数量
    val numItems = itemIndex.count().toInt
    //物品的特征向量
    val itemVectors = itemFeature.flatMap(x => x._2)
    //物品id，物品索引
    val itemIndex_tmp = itemIndex.collectAsMap()

    val blasSim = new BlasSim(numItems, rank, itemVectors, itemIndex_tmp)
    val itemString = itemIndex.map(x => (x._2, x._1)).repartition(8)

    val item_sim_rdd = itemString.map(x => {
      //物品id，
      (x._2, blasSim.getCosinSimilarity((itemFeature(x._1)._2.toVector), 50, None).toList)
    })


    // 广播相似度矩阵
    val item_sim_map = item_sim_rdd.collectAsMap()
    val item_sim_bd: Broadcast[scala.collection.Map[String, List[(String, Double)]]] = spark.sparkContext.broadcast(item_sim_map)
    // 为用户生成推荐列表
    val recTopN = recommend(trainDataFrame, item_sim_bd, 50)
    // 计算召回率
    println(getRecall(recTopN, testDataRrame))


    spark.stop()
  }

}
