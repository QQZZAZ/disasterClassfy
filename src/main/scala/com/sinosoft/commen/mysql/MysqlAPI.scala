package com.sinosoft.commen.mysql

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.{HashMap, LinkedHashMap, ArrayBuffer}


object MysqlAPI {

  lazy val url = "jdbc:mysql://192.168.129.224:3306/biaozhu?serverTimezone=GMT%2B8"
  lazy val username = "zkr_cj"
  lazy val password = "zkrcj123"

  def getMysqlData(spark: SparkSession) = {
    Class.forName("com.mysql.cj.jdbc.Driver")

    val options = new HashMap[String, String]
    options.put("driver", "com.mysql.cj.jdbc.Driver")
    options.put("url", url)
    options.put("user", username)
    options.put("password", password)
    options.put("dbtable", "buchongshuju")

    val studentInfosDF: DataFrame = spark.read.format("jdbc").options(options).load()
    studentInfosDF
  }
}


