package com.sinosoft.commen.algorithm

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.control.Breaks

class TextRankKeyword extends Serializable {
  var numKeyword: Int = 21 /* 关键词个数 */
  var d: Double = 0.85f /* 阻尼系数 */
  var max_iter: Int = 200 /* 最大迭代次数 */
  var min_diff: Double = 0.001f /* 最小变化区间 */
  private final var index:Int = 0
  /* 排序，根据分词对应的权重，由高到底排序 */
  def sortByValue(dataset: RDD[mutable.HashMap[String, Double]]): RDD[Seq[(String, Double)]] = {
    dataset.map(doc => {
      val mapDoc = doc.toSeq
      mapDoc.sortWith(_._2 > _._2).take(numKeyword)
    })
  }

  def rank(document: mutable.HashMap[String, mutable.HashSet[String]]): mutable.HashMap[String, Double] = {
    var score = mutable.HashMap.empty[String, Double]
    val loop  = new Breaks
    loop.breakable {
      for (iter <- 1 to max_iter) {
        val tmpScore = mutable.HashMap.empty[String, Double]
        var max_diff: Double = 0f
        for (word <- document) {
          tmpScore.put(word._1, 1 - d)
          for (element <- word._2) {
            val size = document.apply(element).size
            if(0 == size) println("document.apply(element).size == 0 :element: " + element + "keyword: " + word._1)
            if(word._1.equals(element)) println("word._1.equals(element): " + element + "keyword: " + word._1)
            if ((!word._1.equals(element)) && (0 != size)) {
              /* 计算，这里计算方式可以和TextRank的公式对应起来 */
              tmpScore.put(word._1, tmpScore.apply(word._1) + ((d / size) * score.getOrElse(word._1, 0.0d)))
            }
          }
          /* 取出每次计算中变化最大的值，用于下面得比较，如果max_diff的变化低于min_diff，则停止迭代 */
          max_diff = Math.max(max_diff, Math.abs(tmpScore.apply(word._1) - score.getOrElse(word._1, 0.0d)))
        }
        score = tmpScore
        if(max_diff <= min_diff) loop.break()
      }
    }
    score
  }

  def rank(dataset: RDD[mutable.HashMap[String, mutable.HashSet[String]]]): RDD[mutable.HashMap[String, Double]] = {
    dataset.map(this.rank)
  }

}
