//package com.sinosoft
//
//import com.sinosoft.SecondTemporaryDuty.scalaReadFile
//import com.sinosoft.commen.Hbase.OperationHbase
//import com.sinosoft.hbase.ConnectHbase
//import com.sinosoft.utils.EnumUtil
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.SparkSession
//import org.json.JSONObject
//import redis.clients.jedis.Jedis
//
//object ReadHdfs {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("ReadHdfs")
//      /*.config("redis.host", "10.10.20.92")
//      .config("redis.port", "6382")
//      .config("redis.timeout", "100000")*/
//      //      .master("local[*]")
//      .getOrCreate()
//    //    C:/Users/zdb/Desktop
//    val sc = spark.sparkContext
//    val InfoConf = ConnectHbase.hbase_conf("NEWS_INFO_TABLE")
//    val RDD = ConnectHbase.create_Initial_RDD(sc, InfoConf)
//    val path2 = "hdfs://192.168.129.231:9000/zdb/beijing/1-14heimingdan.txt"
//    //    val path2 = "C:/Users/zdb/Desktop/1-14heimingdan.txt"
//
//    val map = scalaReadFile(spark, "hdfs://192.168.129.231:9000/zdb/beijing/zhuanxiang.txt")
//    //    val map = scalaReadFile(spark, "C:/Users/zdb/Desktop/zhuanxiang.txt")
//    val list = sc.textFile(path2).collect()
//    val bro = sc.broadcast(map)
//    val broBlack = sc.broadcast(list)
//    println("broBlack:" + broBlack.value)
//    println("bro.value:" + bro.value)
//    println("bro.value:" + bro.value.get("1").get)
//    println("===================================")
//
//    val df = RDD.map { case (_, result) =>
//      val rowKey = Bytes.toString(result.getRow)
//      val content = if (result.getValue("wa".getBytes, "m_content".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "m_content".getBytes)) else ""
//      (rowKey, content)
//    }
//    val keyRDD = df.repartition(100).map(f => {
//      val rowkey = f._1
//      val cont = f._2
//      import scala.util.control._
//      // 创建 Breaks 对象
//      val loop = new Breaks;
//      // 在 breakable 中循环
//      val set = broBlack.value
//      var flag = true
//      loop.breakable {
//        for (reg <- set) {
//          if (cont.contains(reg)) {
//            flag = false
//            loop.break
//          }
//        }
//      }
//      (rowkey, flag, cont)
//    }).filter(_._2).map(f => (f._1, f._3))
//
//    val rdds = keyRDD
//      .map(f => (f._1, f._2))
//      .map(f => {
//        //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
//        val rowkey = f._1
//        val cont = f._2
//        import scala.util.control._
//        // 创建 Breaks 对象
//        val loop = new Breaks;
//        // 在 breakable 中循环
//        val Fset = bro.value.get("1").get
//        val set = bro.value
//        var flag = false
//        loop.breakable {
//          for (vv <- Fset) {
//            if (cont.contains(vv)) {
//              flag = true
//              loop.break
//            }
//          }
//          for ((k, set) <- set) {
//            if (cont.contains(k)) {
//              for (reg <- set) {
//                if (cont.contains(reg)) {
//                  //                  println("B类：" + cont + "|||||" + reg + "===" + k)
//                  flag = true
//                  loop.break
//                }
//              }
//            }
//
//          }
//        }
//        (rowkey, flag, cont)
//      }).filter(_._2).map(_._1)
//    rdds.map(f => {
//      val conn = OperationHbase.createHbaseClient()
//      val map = OperationHbase.QueryByConditionTest("NEWS_INFO_TABLE", f, conn)
//      val json = new JSONObject()
//      import scala.collection.JavaConversions._
//      map.foreach(dd => {
//        json.put(dd._1, dd._2)
//      })
//      json.put("rowkey", f)
//      if (conn != null) {
//        conn.close()
//      }
//      json.toString
//    }).saveAsTextFile("hdfs://192.168.129.231:9000/zdb/tianjin")
//    spark.close()
//    sc.stop()
//  }
//}
