package com.sinosoft

import com.sinosoft.utils.MyUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

/**
  * Created by guo on 2020/2/3.
  */
object HdfsToSql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
    val sc = spark.sparkContext
    weibo(sc)

    sc.stop()
    val endTime = System.currentTimeMillis()
    println(this.getClass.getName + " 运行时间: " + (endTime - startTime))
  }
  def weibo(sc: SparkContext): Unit = {
    sc.textFile("/zdb/beijing/wb/result")
      .map(x => {
        val jo = new JSONObject(x)
        val rowkey = if (jo.isNull("rowkey")) "" else MyUtil.getMD5(jo.getString("rowkey"))
        val content = if (jo.isNull("wb_content")) "" else jo.getString("wb_content")
        val title = if (jo.isNull("title")) "" else jo.getString("title")
        val town = if (jo.isNull("town")) "" else jo.getString("town")
        val residential_quarters = if (jo.isNull("residential_quarters")) "" else jo.getString("residential_quarters")
        val village = if (jo.isNull("village")) "" else jo.getString("village")
        val key_areas = if (jo.isNull("key_areas")) "" else jo.getString("key_areas")
        val tendentiousness = if (jo.isNull("wb_tendentiousness")) "" else jo.getString("wb_tendentiousness")
        val emotional_words = if (jo.isNull("emotional_words")) "" else jo.getString("emotional_words")
        val publish_time = if (jo.isNull("wb_pubtime")) "" else jo.getString("wb_pubtime")
        val relation = if (jo.isNull("relation")) "" else jo.getString("relation")
        val URL = if (jo.isNull("wb_weibo_url")) "" else jo.getString("wb_weibo_url")
        val number_of_similar_articles = if (jo.isNull("number_of_similar_articles")) "" else jo.getString("number_of_similar_articles")
        val similar_articles = if (jo.isNull("similar_articles")) "" else jo.getString("similar_articles")
        val special_classification = if (jo.isNull("special_classification")) "" else jo.getString("special_classification")
        val media_type = if (jo.isNull("media_type")) "" else jo.getString("media_type")


        val result = "insert into `test` (`rowkey`, `content`, `title`, `town`, `residential_quarters`, `village`, `key_areas`, `tendentiousness`, `emotional_words`, `publish_time`, `relation`, `URL`, `number_of_similar_articles`, `similar_articles`, `special_classification`, `media_type`) values(\'" + rowkey + "\',\'" + content + "\',\'" + title + "\',\'" + town + "\',\'" + residential_quarters + "\',\'" + village + "\',\'" + key_areas + "\',\'" + tendentiousness + "\',\'" + emotional_words + "\',\'" + publish_time + "\',\'" + relation + "\',\'" + URL + "\',\'" + number_of_similar_articles + "\',\'" + similar_articles + "\',\'" + special_classification + "\',\'" + media_type + "\');"
        result
      }).repartition(200).saveAsTextFile("")
  }

  def tieba(sc: SparkContext): Unit = {
    sc.textFile("/zdb/beijing/tb/result")
      .map(x => {
        val jo = new JSONObject(x)
        val rowkey = if (jo.isNull("rowkey")) "" else MyUtil.getMD5(jo.getString("rowkey"))
        val content = if (jo.isNull("tieba_fl_content")) "" else jo.getString("tieba_fl_content")
        val title = if (jo.isNull("tieba_fl_title")) "" else jo.getString("tieba_fl_title")
        val town = if (jo.isNull("town")) "" else jo.getString("town")
        val residential_quarters = if (jo.isNull("residential_quarters")) "" else jo.getString("residential_quarters")
        val village = if (jo.isNull("village")) "" else jo.getString("village")
        val key_areas = if (jo.isNull("key_areas")) "" else jo.getString("key_areas")
        val tendentiousness = if (jo.isNull("tieba_tendentiousness")) "" else jo.getString("tieba_tendentiousness")
        val emotional_words = if (jo.isNull("emotional_words")) "" else jo.getString("emotional_words")
        val publish_time = if (jo.isNull("tieba_fl_publish_time")) "" else jo.getString("tieba_fl_publish_time")
        val relation = if (jo.isNull("relation")) "" else jo.getString("relation")
        val URL = if (jo.isNull("wb_weibo_url")) "" else jo.getString("wb_weibo_url")
        val number_of_similar_articles = if (jo.isNull("number_of_similar_articles")) "" else jo.getString("number_of_similar_articles")
        val similar_articles = if (jo.isNull("similar_articles")) "" else jo.getString("similar_articles")
        val special_classification = if (jo.isNull("special_classification")) "" else jo.getString("special_classification")
        val media_type = if (jo.isNull("media_type")) "" else jo.getString("media_type")


        val result = "insert into `test` (`rowkey`, `content`, `title`, `town`, `residential_quarters`, `village`, `key_areas`, `tendentiousness`, `emotional_words`, `publish_time`, `relation`, `URL`, `number_of_similar_articles`, `similar_articles`, `special_classification`, `media_type`) values(\'" + rowkey + "\',\'" + content + "\',\'" + title + "\',\'" + town + "\',\'" + residential_quarters + "\',\'" + village + "\',\'" + key_areas + "\',\'" + tendentiousness + "\',\'" + emotional_words + "\',\'" + publish_time + "\',\'" + relation + "\',\'" + URL + "\',\'" + number_of_similar_articles + "\',\'" + similar_articles + "\',\'" + special_classification + "\',\'" + media_type + "\');"
        result
      }).repartition(200).saveAsTextFile("")
  }
}
