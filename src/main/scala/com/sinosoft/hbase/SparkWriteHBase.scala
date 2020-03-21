//package com.sinosoft.hbase
//
//import org.apache.spark.SparkConf
//import org.apache.spark.internal.Logging
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
//
//object SparkWriteHBase extends Logging with Serializable {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
//    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//    import spark.implicits._
//    // 注册 自定义UDF
//    SparkUDFs.register(spark)
//
//    spark.sql(
//      s"""
//        |select MD5(user_id) user_id,city_name from dual
//      """.stripMargin)
//      .write
//      .option(HBaseTableCatalog.tableCatalog, ObjectSchema.common_schema()("table", "user_id", "city_name", "cf", "name"))
//      .save()
//
//    spark.stop()
//  }
//}
