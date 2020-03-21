package com.sinosoft.hbase

import org.apache.spark.sql.SparkSession

object SparkUDFs {
  def register(spark: SparkSession) = {
    spark.udf.register("MD5", (user_id: String) => {
      MD5Utils.getInstance().encodeByMD5(user_id, 16)
    })
  }
}
