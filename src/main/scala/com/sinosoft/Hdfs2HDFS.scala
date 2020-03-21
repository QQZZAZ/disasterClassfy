//package com.sinosoft
//
//import com.sinosoft.Hdfs2HDFS.result
//import com.sinosoft.TemporaryDuty.scalaReadFile
//import com.sinosoft.commen.Hbase.OperationHbase
//import com.sinosoft.utils.EnumUtil
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.json.JSONObject
//import redis.clients.jedis.Jedis
//
//import scala.collection.mutable
//
//object Hdfs2HDFS {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("TemporaryDuty")
//      /*.config("redis.host", "10.10.20.92")
//      .config("redis.port", "6382")
//      .config("redis.timeout", "100000")*/
//      //      .master("local[*]")
//      .getOrCreate()
//
//    val sc = spark.sparkContext
//    val path1 = "hdfs://10.10.10.3:9000/zdb/beijing/beijing-keywords-new.txt"
//    val path2 = "hdfs://10.10.30.3:9000/zdb/beijing/beijing-area.txt"
//    //    val path1 = "C:/Users/zdb/Desktop/beijing-keywords.txt"
//    //    val path2 = "C:/Users/zdb/Desktop/beijing-area.txt"
//    val keywordsList = scalaReadFile(spark, path1)
//    val areaList = scalaReadFile(spark, path2)
//    val map = new mutable.HashMap[String, mutable.HashSet[String]]()
//    map.put("keywords", keywordsList)
//    map.put("area", areaList)
//    val bro = sc.broadcast(areaList)
//
//    val rdd = sc.textFile("hdfs://10.10.10.3:9000/test/wbinfo")
////    service(rdd,)
//
//  }
//
//  private def Service(RDD: RDD[String],
//                      bro: Broadcast[mutable.HashSet[String]],
//                      contentKey: String,
//                      pubtimeKey: String,
//                      sourceBeijing: String,
//                      tableName: String,
//                      dir:String
//                     ) = {
//    val df = RDD.map(f=>{
//      val jo = new JSONObject(f)
//      val rowKey = Bytes.toString(result.getRow)
//      val content = if (result.getValue("wa".getBytes, contentKey.getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, contentKey.getBytes)) else ""
//      (rowKey, content)
//    })
//
//    val result = df.repartition(4000).map(f => {
//
//      val jedis = new Jedis(EnumUtil.redisUrl, EnumUtil.redisPost, 2000000)
//      jedis.incr("test:wx:all")
//      if (jedis != null) {
//        jedis.close()
//      }
//      var flag = false
//      //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
//
//      val rowkey = f._1
//      val cont = f._2
//      import scala.util.control._
//      // 创建 Breaks 对象
//      val loop = new Breaks;
//      var result = ""
//      // 在 breakable 中循环
//      loop.breakable {
//        for (reg <- bro.value) {
//          if (cont.contains(reg)) {
//            flag = true
//            result = rowkey
//            loop.break
//          }
//        }
//      }
//      /*if (flag == false) {
//        put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("o_status"), Bytes.toBytes("1"))
//      }
//      (new ImmutableBytesWritable, put)*/
//      (result, flag, cont)
//    }).filter(_._2).map(_._1)
//
//    /*val keyRDD = result.map(f => {
//      //      val list = List("华晨宇", "巨星", "明星", "小鲜肉", "欧巴")
//      val rowkey = f._1
//      val cont = f._3
//      val put = new Put(Bytes.toBytes(rowkey))
//      import scala.util.control._
//      // 创建 Breaks 对象
//      val loop = new Breaks;
//      var result = ""
//      // 在 breakable 中循环
//      loop.breakable {
//        for (reg <- bro.value.get("area").get) {
//          if (cont.contains(reg)) {
//            result = rowkey
//            loop.break
//          }
//        }
//      }
//      result
//    }).filter(!_.equals(""))*/
//
//    result.repartition(500).map(f => {
//      val conn = OperationHbase.createHbaseClient()
//      val map = OperationHbase.QueryByConditionTest(tableName, f, conn)
//      if (!map.isEmpty) {
//        val jedis = new Jedis(EnumUtil.redisUrl, EnumUtil.redisPost, 2000000)
//        jedis.incr("test:wx:export")
//        if (jedis != null) {
//          jedis.close()
//        }
//        map.put("sourceBeijing", sourceBeijing)
//        //            OperationHbase.insertData("zdbtest", map, x._1, conn)
//
//      }
//      val json = new JSONObject()
//      import scala.collection.JavaConversions._
//      map.foreach(dd=>{
//        json.put(dd._1,dd._2)
//      })
//      json.put("rowkey",f)
//      if (conn != null) {
//        conn.close()
//      }
//      json.toString
//    }).saveAsTextFile("hdfs://10.93.205.13:9000/zdb/"+dir)
//  }
//}
