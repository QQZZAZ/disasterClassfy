//package com.sinosoft
//
//import com.sinosoft.SecondTemporaryDuty.scalaReadFile
//import com.sinosoft.hbase.ConnectHbase
//import com.sinosoft.utils.{EnumUtil, MyUtil}
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.sql.SparkSession
//import redis.clients.jedis.Jedis
//
//object XJZWNews {
//  def main(args: Array[String]): Unit = {
//   val spark = SparkSession.builder().appName("XJZWNews")
//     .getOrCreate()
//
//    val sc = spark.sparkContext
//    val path2 = "hdfs://10.93.205.13:9000/zdb/beijing/1-17heimingdan.txt"
//    val path3 = "hdfs://10.93.205.13:9000/zdb/beijing/1-18heimingdan.txt"
//
//    //    val map = scalaReadFile(spark, "hdfs://10.10.30.3:9000/zdb/beijing/zhuanxiang.txt")
//    val map = scalaReadFile(spark, "hdfs://10.93.205.13:9000/zdb/beijing/zhuanxiang.txt")
//    val list = sc.textFile(path2).collect()
//    val wlist = sc.textFile(path3).collect()
//
//    val bro = sc.broadcast(map)
//    val broBlack = sc.broadcast(list)
//    val broWhite = sc.broadcast(wlist)
//    println("broBlack:" + broBlack.value)
//
//    val WBInfoConf = ConnectHbase.hbase_conf("INFO_TABLE")
//    val WBRDD = ConnectHbase.create_Initial_RDD(sc, WBInfoConf)
//    val df = WBRDD.map { case (_, result) =>
//      val rowKey = Bytes.toString(result.getRow)
//      val content = if (result.getValue("wa".getBytes, "web_info_content".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "web_info_content".getBytes)) else ""
//      val web_publish_date = if (result.getValue("wa".getBytes, "web_publish_date".getBytes) != null) MyUtil.timeTransformation(Bytes.toString(result.getValue("wa".getBytes, "web_publish_date".getBytes))) else 0l
//      (rowKey, content, web_publish_date)
//    }
//
//
//    import spark.implicits._
//    val keyRDD = df.filter(f=> f._3 < 1577808000000l && f._3 > 1546272000000l )
//      .repartition(200)
//      .map(f=>(f._1,f._2))
//      .map(f => {
//        //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
//        val rowkey = f._1
//        val cont = f._2
//        val jedis = new Jedis(EnumUtil.redisUrl, EnumUtil.redisPost, 2000000)
//        jedis.incr("test:news:all:export")
//        if (jedis != null) {
//          jedis.close()
//        }
//        import scala.util.control._
//        // 创建 Breaks 对象
//        val loop = new Breaks;
//        // 在 breakable 中循环
//        val set = broBlack.value
//        var flag = true
//        loop.breakable {
//          for (reg <- set) {
//            if (cont.contains(reg)) {
//              flag = false
//              loop.break
//            }
//          }
//        }
//        (rowkey, flag, cont)
//      }).filter(_._2).map(f => (f._1, f._3))
//
//
//    val rdds = keyRDD
//      .coalesce(1000)
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
//        //        set.remove("1")
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
//                  /*val jedis = new Jedis(EnumUtil.redisUrl, EnumUtil.redisPost, 2000000)
//                  jedis.incr("test:wb:b:export")
//                  if (jedis != null) {
//                    jedis.close()
//                  }*/
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
//      }).filter(_._2).map(f => {
//      var flag = false
//      val cont = f._3
//      for (reg <- broWhite.value) {
//        if (reg.trim.size > 0 && cont.contains(reg)) {
//          flag = true
//        }
//      }
//      (f._1, flag)
//    }).filter(_._2)
//    // .map(_._1).foreach(println(_))
//
//    val dd2 = rdds.map(_._1)
//    dd2.saveAsTextFile("hdfs://10.93.205.13:9000/zdb/beijing/news/zb")
//  }
//}
