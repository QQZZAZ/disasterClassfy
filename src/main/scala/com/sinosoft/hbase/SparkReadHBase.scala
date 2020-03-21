//package com.sinosoft.hbase
//
//import org.apache.spark.SparkConf
//import org.apache.spark.internal.Logging
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
//
//object SparkReadHBase extends Logging with Serializable {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
//    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//    import spark.implicits._
//
//    val df = spark.read
//      .option(HBaseTableCatalog.tableCatalog, ObjectSchema.common_schema()("table", "user_id", "city_name", "cf", "name"))
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//
//    df.show()
//    spark.stop()
//  }
//}
