package com.sinosoft.scheduler

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 是哟个反射方式将RDD转换为Dataframe
  */
object RDD2DataFrameReflection {
  lazy val url = "jdbc:mysql://localhost:3306/spark?serverTimezone=GMT%2B8"
  lazy val table = "ACTIVE"
  lazy val username = "root"
  lazy val password = "741624"
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("RDD2DataFrameReflection")
      .getOrCreate();
    val sqlContext = spark.sqlContext
    //手动添加依赖，将RDD转换为DataFrame的依赖
    import sqlContext.implicits._
    val sc = spark.sparkContext

    val students = sc.textFile("C:\\Users\\zdb\\Desktop\\students.txt",1)
      .map(line=>line.split(",")).map(arr=>Student(arr(0).trim.toInt,arr(1).trim,arr(2).trim.toInt,arr(3)))
    //直接调用隐式转换，将rdd转换成dataframe
    val DF = students.toDF()
//    Seq(1, 2, 3).toDS().show()
    //注册临时表
    DF.createOrReplaceTempView("students")
    val tenegerDF = sqlContext.sql("select id,first(name) name,sum(age) age,FROM_UNIXTIME(first(time)) time from students group by id")
      .select("name","id","age","time")
    tenegerDF.show()
    val props = new Properties
    props.put("user", username)
    props.put("password", password)
    props.put("driver","com.mysql.cj.jdbc.Driver")
    tenegerDF.write.mode(SaveMode.Append).jdbc(url,table,props)


//    tenegerDF.describe("name","age")
//    tenegerDF.filter(f=>f(2).toString.toInt > 18).show()
  }
  case class Student(id:Int,name:String,age:Int,time:String)
}
