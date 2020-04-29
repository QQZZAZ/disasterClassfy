package com.sinosoft.hbase

import org.apache.spark.sql.SparkSession

object SparkUDFs {
  def register(spark: SparkSession) = {
    //这种写法有待放到集群中验证，看是否会获取不到表中的数据
    import spark.implicits._
    val df = spark.createDataset(List("1111", "22222"))
    val broadcast_df_standard = spark.sparkContext.broadcast(df.as("df"))
    broadcast_df_standard.value.createOrReplaceTempView("table")
    spark.catalog.cacheTable("table")
//    spark.catalog.uncacheTable("table") 用完之后记得释放
    spark.udf.register("test", (user_id: String) => {
      spark.sql("select * from table")
        .foreach(f => {
          val reg = f.getAs[String](0)
          if (reg.equals(user_id)) {
            MD5Utils.getInstance().encodeByMD5(user_id, 8)
          }
        })
    })
  }
}
