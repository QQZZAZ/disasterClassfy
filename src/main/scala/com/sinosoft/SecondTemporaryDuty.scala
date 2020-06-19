package com.sinosoft

import java.text.SimpleDateFormat
import java.util.Date

import com.sinosoft.ReadHDFSXJ.analysize
import com.sinosoft.commen.Hbase.OperationHbase
import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.utils.{EnumUtil, MyUtil}
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, ListBuffer}
import scala.io.Source
import scala.util.control.Breaks

object SecondTemporaryDuty {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("SecondTemporaryDuty")
      //      .config("spark.executor.memory", "3g")
//      .master("local[*]")
      .getOrCreate()
    //    C:/Users/zdb/Desktop

    //    println(bro.value)
    val sc = spark.sparkContext

    //    val path2 = "hdfs://10.10.30.3:9000/zdb/beijing/heimingdan0213.txt"
    //    val path2 = "hdfs://10.93.205.13:9000/zdb/beijing/heimingdan213.txt"
    //    val path3 = "hdfs://10.93.205.13:9000/zdb/beijing/1-18baimingdan.txt"
    //    val path3 = "hdfs://10.10.30.3:9000/zdb/beijing/baimingdan213.txt"
    //    val path3 = "hdfs://10.93.205.13:9000/zdb/beijing/baimingdan204.txt"

    val df = spark.read.textFile("C:/Users/zdb/Desktop/s1")
    val map = scalaReadFile(spark, "C:/Users/zdb/Desktop/zhuanxiang_keywords.txt")
    //    val map = scalaReadFile(spark, "hdfs://10.93.205.13:9000/zdb/beijing/zhuanxiangKeyword204.txt")
    //        val map = scalaReadFile(spark, "C:/Users/zdb/Desktop/zhuanxiangKeyword204.txt")
    val bro = sc.broadcast(map)
    //    bro.value.get("重点关键词&&&").foreach(println(_))
    //    bro.value.foreach(println(_))

    import spark.implicits._
    val result = df.flatMap(f => {
      val list = new ListBuffer[(String, Int)]()
      val cont = f
      import scala.util.control._
      // 创建 Breaks 对象
      val loop = new Breaks;
      // 在 breakable 中循环
      val Fset = bro.value.get("重点关键词&&&").get
      val map = bro.value
      //        set.remove("1")
      var F_flag = ""
      var T2_flag = ""
      var T3_flag = ""

      for (vv <- Fset) {
        if (cont.contains(vv)) {
          F_flag = vv
          list.append((F_flag, 1))
        }
      }
      for ((k, set2) <- map) {
        if (cont.contains(k)) {
          T2_flag = k
          for (reg <- set2) {
            if (cont.contains(reg)) {
              T3_flag = reg
              list.append((T2_flag + " " + T3_flag, 1))
            }
          }
        }

      }
      for((k,v) <-list)
        yield {
          (k,v)
        }
    })

    val list = result.rdd.reduceByKey(_ + _).collect()
    list.sortBy(_._2)
      .foreach(println(_))
    println(list.map(_._2).reduce(_ + _) + "条数据")

    spark.close()
    sc.stop()
  }


  private def analysize(spark: SparkSession,
                        sc: SparkContext,
                        broBlack: Broadcast[Array[String]],
                        broWhite: Broadcast[Array[String]],
                        bro: Broadcast[HashMap[String, HashSet[String]]],
                        source: String,
                        dir: String,
                        dirLoad: String,
                        num: Int,
                        df: DataFrame
                       ) = {
    import spark.implicits._
    val black = sc.longAccumulator("black")

    val acc = sc.longAccumulator("ACC")
    val zx = sc.longAccumulator("zhuanxiang")

    //命中黑名单得删除 23756560
    if (source.equals("news")) {
      val keyRDD = df
        //      .coalesce(2000)
        .repartition(num)
        .map(f => {
          //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
          val rowkey = f.getAs[String](0)
          val cont = f.getAs[String](1)

          import scala.util.control._
          // 创建 Breaks 对象
          val loop = new Breaks;
          // 在 breakable 中循环
          val set = broBlack.value
          var flag = true
          loop.breakable {
            for (reg <- set) {
              if (cont.contains(reg)) {
                flag = false
                loop.break
              }
            }
          }
          (rowkey, flag, cont)
        }).filter(f => {
        if (f._2) {
          black.add(1l)
        }
        f._2
      }).map(f => (f._1, f._3))
      //      .coalesce(00)
      //      .write.parquet("hdfs://10.10.30.3:9000/zdb/beijing/news/tmp")


      val rdds = keyRDD
        .map(f => {
          //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
          val rowkey = f._1
          val cont = f._2
          import scala.util.control._
          // 创建 Breaks 对象
          val loop = new Breaks;
          // 在 breakable 中循环
          val Fset = bro.value.get("重点关键词&&&").get
          val map = bro.value
          //        set.remove("1")
          var flag = false
          loop.breakable {
            for (vv <- Fset) {
              if (cont.contains(vv)) {
                zx.add(1l)
                flag = true
                loop.break
              }
            }
            for ((k, set2) <- map) {
              if (cont.contains(k)) {
                //              flag = true
                //              loop.break
                for (reg <- set2) {
                  if (cont.contains(reg)) {
                    zx.add(1l)
                    flag = true
                    loop.break
                  }
                }
              }

            }
          }
          (rowkey, flag, cont)
        }).filter(_._2)
        .map(f => (f._1))
      //        .map(f => (f._1, f._3))

      /* val wrdd = rdds.map(f => {
         //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
         val rowkey = f._1
         val cont = f._2
         import scala.util.control._
         // 创建 Breaks 对象
         val loop = new Breaks;
         // 在 breakable 中循环
         val set = broWhite.value
         //        set.remove("1")
         var flag = false
         loop.breakable {
           for (vv <- set) {
             if (cont.contains(vv)) {
               acc.add(1l)
               flag = true
               loop.break
             }
           }
         }
         (rowkey, flag, cont)
       }).filter(_._2).map(f => (f._1))*/

      val dd2 = rdds.rdd
      dd2.saveAsTextFile("hdfs://10.10.30.3:9000/zdb/beijing/" + source + "/" + dirLoad)
      println(source + "black总计:" + black.value)
      println(source + "zx总计:" + zx.value)
    } else {
      val keyRDD = spark.read.parquet("hdfs://10.10.30.3:9000/zdb/beijing/" + source + "/" + dir)
        //      .coalesce(2000)
        //      .repartition(4000)
        .map(f => {
        //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
        val rowkey = f.getAs[String](0)
        val cont = f.getAs[String](1)

        import scala.util.control._
        // 创建 Breaks 对象
        val loop = new Breaks;
        // 在 breakable 中循环
        val set = broBlack.value
        var flag = true
        loop.breakable {
          for (reg <- set) {
            if (cont.contains(reg)) {
              flag = false
              loop.break
            }
          }
        }
        (rowkey, flag, cont)
      }).filter(f => {
        if (f._2) {
          black.add(1l)
        }
        f._2
      }).map(f => (f._1, f._3))

      val rdds = keyRDD
        .map(f => {
          //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
          val rowkey = f._1
          val cont = f._2
          import scala.util.control._
          // 创建 Breaks 对象
          val loop = new Breaks;
          // 在 breakable 中循环
          val Fset = bro.value.get("重点关键词&&&").get
          val map = bro.value
          //        set.remove("1")
          var flag = false
          loop.breakable {
            for (vv <- Fset) {
              if (cont.contains(vv)) {
                zx.add(1l)
                flag = true
                loop.break
              }
            }
            for ((k, set2) <- map) {
              if (cont.contains(k)) {
                //              flag = true
                //              loop.break
                for (reg <- set2) {
                  if (cont.contains(reg)) {
                    zx.add(1l)
                    flag = true
                    loop.break
                  }
                }
              }

            }
          }
          (rowkey, flag, cont)
        }).filter(_._2)
        .map(f => (f._1))
      //        .map(f => (f._1, f._3))

      /*val wrdd = rdds.map(f => {
        //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
        val rowkey = f._1
        val cont = f._2
        import scala.util.control._
        // 创建 Breaks 对象
        val loop = new Breaks;
        // 在 breakable 中循环
        val set = broWhite.value
        //        set.remove("1")
        var flag = false
        loop.breakable {
          for (vv <- set) {
            if (cont.contains(vv)) {
              acc.add(1l)
              flag = true
              loop.break
            }
          }
        }
        (rowkey, flag, cont)
      }).filter(_._2).map(f => (f._1))*/

      val dd2 = rdds.rdd
      dd2.saveAsTextFile("hdfs://10.10.30.3:9000/zdb/beijing/" + source + "/" + dirLoad)
      println(source + "black总计:" + acc.value)
      println(source + "zx总计:" + zx.value)
      println(source + "最终总计:" + black.value)

    }

  }

  def scalaReadFile(spark: SparkSession, path: String) = {
    val file = spark.read.textFile(path)
    val Allmap = new mutable.HashMap[String, mutable.HashMap[String, HashSet[String]]]()
    val Smap = new mutable.HashMap[String, String]()
    val Fmap = new mutable.HashMap[String, String]()
    val Tmap = new mutable.HashMap[String, HashSet[String]]()

    val Fset = new mutable.HashSet[String]()

    val list = file.collect().iterator
    var index = 0
    var varr = ""
    var key = ""
    while (list.hasNext) {
      val value = list.next()
      /*if (value.equals("5-1->开墙")) {
        println("ok")
      }*/
      if (value.trim.size > 0) {
        val arr = value.split("->")
        if (arr(0).size <= 2) {
          Fset.add(arr(1))
        } else if (arr(0).split("-").size == 2) {
          Smap.put(arr(0), arr(1))
        } else {
          index = arr(0).lastIndexOf("-")
          varr = arr(0).substring(0, index)
          try {
            key = Smap.get(varr).get

          } catch {
            case e: Exception => println(varr)
          }
          if (Tmap.contains(key)) {
            val Sset = Tmap.get(key).get
            Sset.add(arr(1))
            //            Tmap.put(key, rset)
          } else {
            val set = new mutable.HashSet[String]()
            set.add(arr(1))
            Tmap.put(key, set)
          }
        }
      }
    }

    Tmap.put("重点关键词&&&", Fset)
    Tmap
  }
}
