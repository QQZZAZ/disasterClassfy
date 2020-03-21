package com.sinosoft.service

import com.sinosoft.commen.Hbase.OperationHbase
import redis.clients.jedis.Jedis

object PopRedis {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("192.168.129.14", 6379, 6000)

     while(jedis.llen("keylist") > 0){
       val ste = jedis.lpop("keylist")
       println(ste)
     }
    while(jedis.llen("zdbtest") > 0){
      val ste = jedis.lpop("zdbtest")
      println(ste)
    }
    /*val lrn = jedis.llen("keylist")
    import scala.collection.JavaConverters._
    val list = jedis.lrange("keylist", 0, lrn).asScala
    for (key <- list) {
      println(key)
      val con = OperationHbase.createHbaseClient()
      val map = OperationHbase.QueryByConditionTest("INFO_TABLE",key,con)
//      println(map)
      val pubtime = map.get("web_info_push_time")
      println(pubtime)
    }*/

    jedis.close()
  }
}
