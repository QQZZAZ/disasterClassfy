package com.sinosoft

import java.io.IOException
import java.sql.Timestamp

import breeze.util.BloomFilter
import com.google.common.hash.{Funnels, Hashing}
import com.sinosoft.CreateTableWeek.tjkafkaurl
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.{Row, SparkSession}
import org.json.JSONObject
import redis.clients.jedis.{Jedis, Pipeline}


object PostsNumberCount {
  lazy private val expectedInsertions = 100000000
  lazy private val fpp = 0.001F
  lazy private val numBits = optimalNumOfBits(expectedInsertions, fpp)
  lazy private val numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits)
  lazy private val redisKeyPrefix = "bf:"

  def main(args: Array[String]): Unit = {
    /* val weburl = GetPlaceWeb.queryWebAll("web")
     val wechaturl = GetPlaceWeb.queryWebAll("WeChat")
     val miniVediourl = GetPlaceWeb.queryWebAll("douyin")*/
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("PostsNumberCount")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", tjkafkaurl)
      .option("subscribe", "sc_kafka_content")
      //从kafka的每个partition最多只取20000条数据，当服务器宕机时使用，因为会积压大量数据
      .option("spark.streaming.backpressure.enabled", true) //开启内部背压机制与上面的maxRatePerPartition组合使用
      .option("spark.speculation", true)
      .option("failOnDataLoss", false)
      .load()
    df.printSchema()

    val qa = df.select("value")
      .selectExpr("CAST(value AS STRING)")
      .as[String].map(f => {
      var rowkey = ""
      var tableName = ""
      var time: Timestamp = null
      try {
        val js = new JSONObject(f)
        rowkey = js.getString("infoRowkey")
        tableName = js.getString("infoTableName")
      } catch {
        case e: Exception => e.printStackTrace()
      }
      (rowkey, tableName)
    }).filter(f => f._2.equals("INFO_TABLE") || f._2.equals("MIN_VIDEO_INFO_TABLE")
      || f._2.equals("WECHAT_INFO_TABLE")).toDF("rowkey", "tableName")

    val ddf = qa
      .mapPartitions(f => {
        val jedis = new Jedis("192.168.129.14", 6379)
//        val jedis = new Jedis("localhost", 6379)
        val p1: Pipeline = jedis.pipelined()
        p1.multi()
        val tuple = f.map(x => {
          val flag = isExist("duplicateKey", x.getAs[String](0), p1)
          (x.getAs[String](0), flag, x.getAs[String](1))
        })
        //      p1.exec()//提交事务
        p1.sync() //关闭
        p1.close()
        jedis.close()
        tuple
      }).filter(!_._2).toDF("key", "flag", "tableName")

    /* val hbasedf = ddf.mapPartitions(f => {
       val hc = OperationHbase.createHbaseClient
       val tuple = f.map(x => {
         val map = OperationHbase.QueryByConditionTest(x.getAs[String](2), x.getAs[String](0), hc)
         val pubtime = map.get("wb_pubtime")
         var dateYM, dateD = ""
         if (pubtime != null && !pubtime.equals("")) {
           val tmp = pubtime.split(" ")(0)
           if (tmp.contains("-")) {
             val split = tmp.split("-")
             if (split.length == 3) {
               //切分出年月日
               dateYM = split(0) + "_" + split(1) + "_" + split(2)
               dateD = split(2)
             }
           }
         }
         //todo wb_pubtime 要年月 日
         (map.get("web_info_website_url"), dateYM, dateD)
       })
       hc.close()
       tuple
     }).toDF("key", "table", "xxx")*/
    //todo 访问redis 的过程数据

    val qa2 = ddf.writeStream.option("truncate", false)
      //      .format("csv") // can be "orc", "json", "csv","parquet" etc.
      /*.option("path", "C:\\Users\\zdb\\Desktop\\1")
      .option("checkpointLocation","C:\\Users\\zdb\\Desktop\\2")*/
      .outputMode("append")
      .format("console")
//      .foreach(new DropDunplicatesHbaseSink)
      .start()
    qa2.awaitTermination()

  }

  private def optimalNumOfBits(n: Long, p: Double) = {
    var a = 0d
    if (p == 0)
      a = Double.MinPositiveValue
    else {
      a = p
    }
    (-n * Math.log(a) / (Math.log(2) * Math.log(2))).toLong
  }

  private def optimalNumOfHashFunctions(n: Long, m: Long) = Math.max(1, (m.toDouble / n * Math.log(2)).round)

  /**
    * 判断keys是否存在于集合where中
    */
  def isExist(where: String, key: String, pipeline: Pipeline) = {
    val indexs = getIndexs(key)
    var result = false
    //这里使用了Redis管道来降低过滤器运行当中访问Redis次数 降低Redis并发量
    try {
      for (index <- indexs) {
        //        pipeline.syncAndReturnAll
        pipeline.getbit(getRedisKey(where), index)
      }
      result = pipeline.syncAndReturnAll().contains(true)
      if (!result) {
        put(where, key, pipeline)
        val result = pipeline.ttl(getRedisKey(where))
        pipeline.sync()
        //        println(result.get())
        if (result.get() < 0) {
          pipeline.expire(getRedisKey(where), 86400)
        }
      }
    } catch {
      case e: Exception => {
        println("invalid key: " + key)
        e.printStackTrace()
      }
    }
    result
  }


  /**
    * 将key存入redis bitmap
    */
  private def put(where: String, key: String, pipeline: Pipeline): Unit = {
    val indexs = getIndexs(key)
    //这里使用了Redis管道来降低过滤器运行当中访问Redis次数 降低Redis并发量
    //    val pipeline = jedis.pipelined
    for (index <- indexs) {
      pipeline.setbit(getRedisKey(where), index, true)
    }
  }

  /**
    * 根据key获取bitmap下标 方法来自guava
    */
  def getIndexs(key: String) = {
    val hash1 = hash(key)
    //    println(hash1)
    val hash2 = hash1 >>> 32
    val result = new Array[Long](numHashFunctions.toInt)
    var i = 0
    while (i < numHashFunctions) {
      var combinedHash = hash1 + i * hash2
      //      println("combinedHash:"+combinedHash)
      if (combinedHash < 0) {
        combinedHash = ~combinedHash
      }
      result(i) = combinedHash % numBits
      i += 1
    }
    result
  }

  /**
    * 获取一个hash值 方法来自guava
    */
  def hash(key: String) = {
    Hashing.murmur3_128.hashString(key).asLong()
  }

  def getRedisKey(where: String) = redisKeyPrefix + where

}
