package com.sinosoft

import com.sinosoft.TemporaryDuty.scalaReadFile
import com.sinosoft.utils.EnumUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json.JSONObject
import redis.clients.jedis.Jedis

object NEWTempDuty {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("NEWTempDuty")
      /*.config("redis.host", "10.10.20.92")
      .config("redis.port", "6382")
      .config("redis.timeout", "100000")*/
      //      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val path1 = "hdfs://10.93.205.13:9000/zdb/beijing/beijing-keywordszuixin.txt"
    val path2 = "hdfs://10.93.205.13:9000/zdb/beijing/aa.txt"
    //        val path1 = "C:/Users/zdb/Desktop/beijing-keywords.txt"
    //    val path2 = "C:/Users/zdb/Desktop/beijing-area.txt"
    val keywordsList = scalaReadFile(spark, path1)
    val blackList = scalaReadFile(spark, path2)
    //    val map = new mutable.HashMap[String, mutable.HashSet[String]]()
    //    map.put("keywords", keywordsList)
    //    map.put("area", areaList)
    val bro = sc.broadcast(keywordsList)
    println("bro.value:"+bro.value)
    val broBlack = sc.broadcast(blackList)
    println("broBlack.value:"+broBlack.value)

    val rdd =sc.textFile("hdfs://10.93.205.13:9000/zdb/wx")
    rdd.map(f=>{
      val json = new JSONObject(f)
      val content = if (json.isNull("wx_content")) "" else json.getString("wx_content")
      var flag = false
      for(key <- bro.value){
        if(content.contains(key)){
          flag = true
        }
      }
      (json.toString,flag)
    }).filter(_._2).map(_._1)
      .map(f=>{
        val json = new JSONObject(f)
        val content = if (json.isNull("wx_content")) "" else json.getString("wx_content")
        var flag = true
        for(key <- broBlack.value){
          if(content.contains(key)){
            val jedis = new Jedis(EnumUtil.redisUrl, EnumUtil.redisPost, 2000000)
            jedis.incr("test:wx:export")
            if (jedis != null) {
              jedis.close()
            }
            flag = false
          }
        }
        (json.toString,flag)
      }).filter(_._2).map(_._1)
      .repartition(150)
      .saveAsTextFile("hdfs://10.93.205.13:9000/zdb/wx2")

    spark.close()
    sc.stop()


  }
}
