package com.sinosoft

import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.javas.HashAl
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

import scala.collection.mutable
import scala.util.control.Breaks

object Test2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("test2")
      //      .config("redis.host", "192.168.129.211")
      //      .config("redis.port", "6380")
      //      .config("redis.timeout", "100000")
//      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
        val list = spark.read.textFile("hdfs://10.10.30.3:9000/zdb/new_wx_dt.txt").collect()
//    val list = spark.read.textFile("C:/Users/zdb/Desktop/new_wx_dt.txt").collect()

    println(list.size)
    val bro = sc.broadcast(list)

//    val InConf = ConnectHbase.hbase_conf("WECHAT_INFO_TABLE")
//    val RDD = ConnectHbase.create_Initial_RDD(sc, InConf)

    /*val resultDF = RDD.map { case (_, result) => {
      val userID = if (result.getValue("wa".getBytes, "wx_user_name".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "wx_user_name".getBytes)) else ""
      val userName = if (result.getValue("wa".getBytes, "wx_nick_name".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "wx_nick_name".getBytes)) else ""
      val url = if (result.getValue("wa".getBytes, "wx_article_url".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "wx_article_url".getBytes)) else ""
      userID + "%%%&&&" + userName + "%%%&&&" + url
    }
    }

    resultDF.repartition(500).saveAsTextFile("hdfs://10.10.30.3:9000/zdb/data")*/
    val acc = sc.longAccumulator("acc")
    val df = sc.textFile("hdfs://10.10.30.3:9000/zdb/data").repartition(500)
      .filter(_.split("\\%\\%\\%\\&\\&\\&").size == 3)
      .map(f => {
        val start = System.currentTimeMillis()
        val arr = f.split("\\%\\%\\%\\&\\&\\&")
        val wx_user_name = arr(0)
        val wx_nick_name = arr(1)
        val url = arr(2)
        val list = bro.value
        val json = new JSONObject()
        val loop = new Breaks
        loop.breakable {
          for (cont <- list) {
            val arr = cont.split("\\|\\|\\|\\|")
            if (arr.length == 4) {
              var ID = ""
              var WECHAT_NAME = ""
              var wechatid = ""
              var survival_state = ""
              try {
                ID = arr(0).split("=")(1)
                WECHAT_NAME = arr(1).split("=")(1)
                wechatid = arr(2).split("=")(1)
                survival_state = arr(3).split("=")(1)
              } catch {
                case e: Exception => println(ID + " " + WECHAT_NAME + " " + wechatid + " " + survival_state)
              }

              if (wechatid.equals(wx_user_name)) {
                json.put("ID", ID)
                json.put("wechatid", wechatid)
                json.put("wx_user_name", wx_user_name)
                json.put("WECHAT_NAME", WECHAT_NAME)
                json.put("survival_state", survival_state)
                json.put("url", url)
                acc.add(1)
                loop.break
                //              println(ID + "@@@" + WECHAT_NAME + "@@@" + wechatid + "@@@" + survival_state)
              } else if (WECHAT_NAME.equals(wx_nick_name)) {
                json.put("ID", ID)
                json.put("wechatid", wechatid)
                json.put("WECHAT_NAME", WECHAT_NAME)
                json.put("wx_nick_name", wx_nick_name)
                json.put("survival_state", survival_state)
                json.put("url", url)
                //              println(ID + "@@@" + WECHAT_NAME + "@@@" + wechatid + "@@@" + survival_state)
                acc.add(1)
                loop.break
              }
            }
          }
        }
        val end = System.currentTimeMillis()


        if (json.length() > 0) {
          //          println(json.toString)
          json.toString
        } else {
          ""
        }
      }).filter(!_.equals(""))
      .toDF()
    //      .cache()
    df.write.text("hdfs://10.10.30.3:9000/zdb/ahrsult")


    println("Count:" + acc.value)
    spark.close()
    sc.stop()
  }
}
