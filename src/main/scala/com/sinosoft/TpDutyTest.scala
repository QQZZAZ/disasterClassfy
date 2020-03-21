//package com.sinosoft
//
//import com.sinosoft.SecondTemporaryDuty.scalaReadFile
//import com.sinosoft.TemporaryDuty.scalaReadFile
//import com.sinosoft.hbase.ConnectHbase
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable
//import scala.collection.mutable.HashSet
//import scala.util.Random
//import scala.util.control.Breaks
//
//object TpDutyTest {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("TpDutyTest")
//      /*.config("redis.host", "10.10.20.92")
//      .config("redis.port", "6382")
//      .config("redis.timeout", "100000")*/
//      .master("local[*]")
//      .getOrCreate()
//    val ab = "问个事新兴家园楼前的车位是固定的么"
//    val path2 = "C:/Users/zdb/Desktop/heimingdan213.txt"
//    val path3 = "C:/Users/zdb/Desktop/baimingdan204.txt"
//
//    //    val map = scalaReadFile(spark, "hdfs://10.10.30.3:9000/zdb/beijing/zhuanxiang.txt")
//    val map = scalaReadFile(spark, "C:/Users/zdb/Desktop/zhuanxiangKeyword204.txt")
//
//    val sc = spark.sparkContext
//
//    val list = sc.textFile(path2).collect()
//    val wlist = sc.textFile(path3).collect()
//
//    val bro = sc.broadcast(map)
//    val broBlack = sc.broadcast(list)
//    val broWhite = sc.broadcast(wlist)
////    broWhite.value.foreach(println(_))
//    println("broWhite:" + broWhite.value.length)
//
//    //    println("bro.value:" + bro.value)
//    //    println("bro.value:" + bro.value.get("1").get)
//    println("===================================")
//
//    import scala.util.control._
//    // 创建 Breaks 对象
//    val loop = new Breaks;
//    // 在 breakable 中循环
//    val Fset = bro.value.get("重点关键词&&&").get
//    val set = bro.value
//    //        set.remove("1")
//    var flag = false
//    loop.breakable {
//      for (vv <- Fset) {
//        if (ab.contains(vv)) {
//          println("A:" + vv)
//          flag = true
//          loop.break
//        }
//      }
//      for ((k, set2) <- set) {
//        if (ab.contains(k)) {
//          println("命中专项词："+k)
//          /*for (reg <- set2) {
//            if (ab.contains(reg)) {
//              /*val jedis = new Jedis(EnumUtil.redisUrl, EnumUtil.redisPost, 2000000)
//              jedis.incr("test:wb:b:export")
//              if (jedis != null) {
//                jedis.close()
//              }*/
//              println("B类：" + "|||||" + reg + "===" + k)
//              flag = true
//              loop.break
//            }
//          }*/
//        }
//
//      }
//    }
//
//    for(reg <- broWhite.value){
////      println("broWhite"+reg)
//      if(ab.contains(reg)){
//        println("C:"+reg)
//      }
//    }
//    spark.close()
//    sc.stop()
//  }
//
//
//  def scalaReadFile(spark: SparkSession, path: String) = {
//    val file = spark.read.textFile(path)
//    val Allmap = new mutable.HashMap[String, mutable.HashMap[String, HashSet[String]]]()
//    val Smap = new mutable.HashMap[String, String]()
//    val Fmap = new mutable.HashMap[String, String]()
//    val Tmap = new mutable.HashMap[String, HashSet[String]]()
//
//    val Fset = new mutable.HashSet[String]()
//
//    val list = file.collect().iterator
//    var index = 0
//    var varr = ""
//    var key = ""
//    while (list.hasNext) {
//      val value = list.next()
//      /*if (value.equals("5-1->开墙")) {
//        println("ok")
//      }*/
//      if (value.trim.size > 0) {
//        val arr = value.split("->")
//        if (arr(0).size <= 2) {
//          Fset.add(arr(1))
//        } else if (arr(0).split("-").size == 2) {
//
//          Smap.put(arr(0), arr(1))
//
//        } else {
//          index = arr(0).lastIndexOf("-")
//          varr = arr(0).substring(0, index)
//          key = Smap.get(varr).get
//          if (Tmap.contains(key)) {
//            val Sset = Tmap.get(key).get
//            Sset.add(arr(1))
//            //            Tmap.put(key, rset)
//          } else {
//            val set = new mutable.HashSet[String]()
//            set.add(arr(1))
//            Tmap.put(key, set)
//          }
//        }
//      }
//    }
//
//    Tmap.put("重点关键词&&&", Fset)
//    Tmap
//  }
//}
