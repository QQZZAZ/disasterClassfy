package com.sinosoft.algorithm

import scala.collection.mutable.{ArrayBuffer, Map}

/**
  * 生成词向量的TF模型和IDF模型
  */
object TF_IDF_Model {
  /**
    * 计算每个文档的tf值
    * @param wordAll
    * @return Map<String,Float> key是单词 value是tf值
    */
  def tfCalculate(wordAll:String)={
    //存放单词，单词数量
    val dictMap = Map[String,Int]()

    var wordCount = 1

    /**
      * 统计每个单词的数量，并存放到map中去
      * 便于以后计算每个单词的词频
      * 单词的tf=该单词出现的数量n/总的单词数wordCount
      */
    for(word <- wordAll.split(" ")){
      wordCount += 1
      if(dictMap.contains(word)){
        dictMap.put(word,dictMap(word)+1);
      }else{
        dictMap.put(word, 1);
      }
    }
    //存放单词，单词频率
    val tfMap =  dictMap.map(f=>{
      val wordTf = f._2.toFloat / wordCount
      (f._1,wordTf)
    })
    tfMap

  }

  /**
    *
    * @param D 总文档数
    * @param doc_words 每个文档对应的分词
    * @param tfMap 计算好的tf,用这个作为基础计算tfidf
    * @return 每个文档中的单词的tfidf的值
    */
  def idfCalculate(D:Int,doc_word:Map[String,String],tfMap:Map[String,Float])={
    val idfMap = Map[String,Float]()

    for(tfKey <- tfMap.keySet){
      var Dt = 0
      for(doc <- doc_word){
        val words = doc._2.split(" ")
        val wordsList = ArrayBuffer[String]()
        for(word <- words){
          wordsList.append(word)
        }
        if (wordsList.contains(tfKey)) Dt =  Dt + 1
      }
      //总文档数 / 在所有文档中出现某个词的次数
      val idfvalue = Math.log(D.toFloat/Dt).toFloat
      idfMap.put(tfKey,idfvalue)
    }

    idfMap
  }

  def main(args: Array[String]): Unit = {
    val wordsAll = "合肥 工业 大学 高等院校 简称 合 工大 位于 安徽省 省会 合肥市 创建 1945年 秋 1960年 10月 22日 中共中央 批准 全国 重点 大学 教育部 直属 高校 工程 工程 优势 学科 创新 平台 项目 建设 高校 工科 主要 特色 工 理 文 经 管 法 教育 多 学科 综合性 高等院校"
    val tfmap = tfCalculate(wordsAll)
    val map = Map[String,String]("wordsAll" -> wordsAll)
    val idfMap = idfCalculate(2,map,tfmap)
    println(idfMap)

    val words = wordsAll.split(" ")
    val word = words.sorted
    println("========================================")
    for(i <- 0 until(word.length)){
      println(word(i))
    }
  }
}
