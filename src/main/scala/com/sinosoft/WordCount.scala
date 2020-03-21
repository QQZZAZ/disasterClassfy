package com.sinosoft

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.config.ConfigException.ValidationProblem
import org.apache.spark.SparkContext
import org.hamcrest.core.Every
import redis.clients.jedis.Jedis
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try


object WordCount extends SparkJob {
  def main(args: Array[String]) {
    val jedis = new Jedis("192.168.129.211",6380)
    val list = jedis.llen("es:news:insert:info")
    println(list)
    jedis.close()
  }


  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }


  override def runJob(sc: SparkContext, config: Config): Any = {
    sc.setCheckpointDir("hfdfs://192.1268.129.10:/zdb/rddcheckpoint")
    val dd = sc.parallelize(config.getString("input.string").split(" ").toSeq)
    dd.checkpoint()
    dd.foreach(println(_))
    val rsList = dd.map((_,1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).collect
    val s = rsList(0)._2 +","+ rsList(1)._2 +","+ rsList(2)._2
    s
  }
}

