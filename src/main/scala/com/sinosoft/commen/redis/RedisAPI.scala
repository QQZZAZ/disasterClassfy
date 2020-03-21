package com.sinosoft.commen.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

class RedisAPI(sc:SparkContext,pairRDD:RDD[(String,(String,String))]) {
  def setData2RedisList()={
    pairRDD.foreachPartition(it=>{
      val jedis = new Jedis("192.168.129.14")
      //      val jedis = new Jedis("localhost")
      it.foreach(data=>{
        val key = data._1 +"|||||web_type_code"
        //        println(key)
        jedis.lpush("es:news:insert:info",key)
      })
    })
  }
}
