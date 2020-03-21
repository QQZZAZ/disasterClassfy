package com.sinosoft.commen.algorithm

import org.apache.spark.rdd.RDD

import scala.collection.mutable

class TextRankWordSet extends Serializable {
  def transform(document: Iterator[_]): mutable.HashMap[String, mutable.HashSet[String]] ={

    val keyword = mutable.HashMap.empty[String, mutable.HashSet[String]]
    val que = mutable.Queue.empty[String]
    document.foreach { term =>
      val word = term.toString
      if (!word.trim.equals("") && !keyword.contains(word)) {
        /* 初始化，对每个分词分配一个 HashSet 空间*/
        keyword.put(word, mutable.HashSet.empty[String])
      }
      //建立两个大小为5的窗口，每个单词将票投给它身前身后距离5以内的单词
      if(!word.trim.equals("")){
        que.enqueue(word)
        if (que.size > 5) {
          que.dequeue()
        }
      }

      for (w1 <- que) {
        for (w2 <- que) {
          if (!w1.equals(w2)) {
            keyword.apply(w1).add(w2)
            keyword.apply(w2).add(w1)
          }
        }
      }
    }
    keyword
  }

  def transform[D <: Iterator[_]] (dataset: RDD[D]): RDD[mutable.HashMap[String, mutable.HashSet[String]]] = {
    dataset.map(this.transform)
  }
}
