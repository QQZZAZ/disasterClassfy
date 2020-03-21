package com.sinosoft

import com.sinosoft.commen.Hbase.OperationHbase
import com.sinosoft.hbase.ConnectHbase
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

import scala.collection.mutable

object DuplicateArticle {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("DuplicateArticle")
      .config("redis.host", "10.10.20.92")
      .config("redis.port", "6382")
      .config("redis.timeout", "100000")
      .getOrCreate()

    val sc = spark.sparkContext

    val InConf = ConnectHbase.hbase_conf("news_hlw_a")
    val RDD = ConnectHbase.create_Initial_RDD(sc, InConf)
    val OUTConf = ConnectHbase.createJobConf("news_hlw_a", InConf)

    val bInConf = ConnectHbase.hbase_conf("news_hlw_s")
    val bRDD = ConnectHbase.create_Initial_RDD(sc, bInConf)
    val bOUTConf = ConnectHbase.createJobConf("news_hlw_s", bInConf)


    anlysis2(spark, sc, RDD, OUTConf, "news_hlw_a")
    anlysis2(spark, sc, bRDD, bOUTConf, "news_hlw_s")

    spark.close()
    sc.stop()
  }

  private def anlysis(spark: SparkSession,
                      sc: SparkContext,
                      RDD: RDD[(ImmutableBytesWritable, Result)],
                      OUTConf: JobConf,
                      tableNames: String) = {

    val WBRDD = RDD.map { case (_, result) =>
      val rowKey = Bytes.toString(result.getRow)
      val wb_weibo_url = if (result.getValue("wa".getBytes, "wx_article_url".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "wx_article_url".getBytes)) else ""
      val wb_content_md5 = if (result.getValue("wa".getBytes, "wx_content_md5".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "wx_content_md5".getBytes)) else ""
      (rowKey, wb_content_md5, wb_weibo_url)
    }
    /* val rdd = WBRDD
      .map(x => {
        //rowkey
        val rowkey = if (jo.isNull("rowkey")) "" else jo.getString("rowkey")
        //帖子-URL
        val m_page_url = if (jo.isNull("")) "" else jo.getString("tieba_fl_website_url")
        //内容MD5
        var tieba_content_md5 = if (jo.isNull("tieba_content_md5")) "" else jo.getString("tieba_content_md5")

        (rowkey, tieba_content_md5, m_page_url)
      })
*/
    import spark.implicits._
    val filterRDD = WBRDD
      .filter(f => f._3.size != 0 && f._2.size != 0)
      .repartition(300)
      .toDF("rowkey", "md5", "url")

    filterRDD.createOrReplaceTempView("FirstTable")
    filterRDD.show(false)

    //处理每个roeky对应的重复文章的个数，按照文章的url值进行count
    val tableMD5NUM = spark.sql("select f.md5,count(1) as num from FirstTable f group by f.md5")
    tableMD5NUM.createOrReplaceTempView("tableMD5NUM")
    tableMD5NUM.show(false)

    val tableMD5NUMresult = spark.sql("select f.rowkey,t.num from FirstTable f join tableMD5NUM t " +
      "on f.md5 = t.md5")
    tableMD5NUMresult.show(false)

    tableMD5NUMresult.rdd.map(f => {
      val rowkey = f.getAs[String](0)
      val put = new Put(Bytes.toBytes(rowkey))
      val number = f.getAs[Long](1)
      //      println(number)
      put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("number_of_similar_articles"), Bytes.toBytes(number.toString))
      (new ImmutableBytesWritable, put)
    }).filter(!_._2.isEmpty)
      .saveAsHadoopDataset(OUTConf)


    //    tableMD5NUM.createOrReplaceTempView("tableMD5NUM")


    //分析每个rowkey对应的重复文章的列表
    val tableMD5List = WBRDD.map(f => (f._2, f._3))
      .aggregateByKey(scala.collection.mutable.ArrayBuffer[String]())((arr, value) => {
        //2、将value放入集合U中
        arr += value
        //3、将所有的集合进行合并
      }, _.union(_)).map(f => {
      val md5 = f._1
      val set = f._2.take(50)
      var str = new StringBuffer()
      for (s <- set) {
        if (str.length() == 0 && s.size < 100) {
          str.append(s)
        } else {
          str.append(",").append(s)
        }
      }
      (md5, str.toString)
    }).toDF("md5", "keyList")
    tableMD5List.show(false)
    tableMD5List.createOrReplaceTempView("tableMD5List")

    val result = spark.sql("select f.rowkey,t.keyList from FirstTable f join tableMD5List t " +
      "on f.md5 = t.md5")
    result.show(false)

    result.rdd.map(f => {
      val rowkey = f.getAs[String](0)
      val put = new Put(Bytes.toBytes(rowkey))
      val list = if (f.getAs[String](1) == null) "" else f.getAs[String](1)
      put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("similar_articles"), Bytes.toBytes(list))
      (new ImmutableBytesWritable, put)

    }).filter(!_._2.isEmpty)
      .saveAsHadoopDataset(OUTConf)

    tableMD5NUMresult.show(false)
    tableMD5NUM.show(false)

    println("result.rdd:" + result.rdd.count())
    /*import com.redislabs.provider.redis._
    sc.toRedisLIST(
      result.rdd.map(f => {
        val rowkey = f.getAs[String](0)
        //        rowkey + "|||||o_status"
        "{\"rowkey\":" + "\"" + rowkey + "\"" + "," + "\"table\":\"" + tableNames + "\"}"
      }), "cunzhengxiang:data"
    )*/
  }

  private def anlysis2(spark: SparkSession,
                       sc: SparkContext,
                       RDD: RDD[(ImmutableBytesWritable, Result)],
                       OUTConf: JobConf,
                       tableNames: String) = {

    val WBRDD = RDD.map { case (_, result) =>
      val rowKey = Bytes.toString(result.getRow)
      val b_content_md5 = if (result.getValue("wa".getBytes, "b_content_md5".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "b_content_md5".getBytes)) else ""
      (rowKey, b_content_md5)
    }
    /* val rdd = WBRDD
      .map(x => {
        //rowkey
        val rowkey = if (jo.isNull("rowkey")) "" else jo.getString("rowkey")
        //帖子-URL
        val m_page_url = if (jo.isNull("")) "" else jo.getString("tieba_fl_website_url")
        //内容MD5
        var tieba_content_md5 = if (jo.isNull("tieba_content_md5")) "" else jo.getString("tieba_content_md5")

        (rowkey, tieba_content_md5, m_page_url)
      })
*/
    import spark.implicits._
    val filterRDD = WBRDD
      .filter(f => f._2.size != 0)
      .repartition(300)
      .toDF("rowkey", "md5")

    filterRDD.createOrReplaceTempView("FirstTable")
    filterRDD.show(false)

    //处理每个roeky对应的重复文章的个数，按照文章的url值进行count
    val tableMD5NUM = spark.sql("select f.md5,count(1) as num from FirstTable f group by f.md5")
    tableMD5NUM.createOrReplaceTempView("tableMD5NUM")
    tableMD5NUM.show(false)

    val tableMD5NUMresult = spark.sql("select f.rowkey,t.num from FirstTable f join tableMD5NUM t " +
      "on f.md5 = t.md5")
    tableMD5NUMresult.show(false)

    tableMD5NUMresult.rdd.map(f => {
      val rowkey = f.getAs[String](0)
      val put = new Put(Bytes.toBytes(rowkey))
      val number = f.getAs[Long](1)
      //      println(number)
      put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("number_of_similar_articles"), Bytes.toBytes(number.toString))
      (new ImmutableBytesWritable, put)
    }).filter(!_._2.isEmpty)
      .saveAsHadoopDataset(OUTConf)


    //    tableMD5NUM.createOrReplaceTempView("tableMD5NUM")


    //分析每个rowkey对应的重复文章的列表
    val tableMD5List = WBRDD.map(f => (f._2, f._1))
      .aggregateByKey(scala.collection.mutable.ArrayBuffer[String]())((arr, value) => {
        //2、将value放入集合U中
        arr += value
        //3、将所有的集合进行合并
      }, _.union(_)).map(f => {
      val md5 = f._1
      val set = f._2.take(50)
      var str = new StringBuffer()
      for (s <- set) {
        if (str.length() == 0 && s.size < 100) {
          str.append(s)
        } else {
          str.append(",").append(s)
        }
      }
      (md5, str.toString)
    }).toDF("md5", "keyList")
    tableMD5List.show(false)
    tableMD5List.createOrReplaceTempView("tableMD5List")

    val result = spark.sql("select f.rowkey,t.keyList from FirstTable f join tableMD5List t " +
      "on f.md5 = t.md5")
    result.show(false)

    result.rdd.map(f => {
      val rowkey = f.getAs[String](0)
      val put = new Put(Bytes.toBytes(rowkey))
      val list = if (f.getAs[String](1) == null) "" else f.getAs[String](1)
      put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("similar_articles"), Bytes.toBytes(list))
      (new ImmutableBytesWritable, put)

    }).filter(!_._2.isEmpty)
      .saveAsHadoopDataset(OUTConf)

    tableMD5NUMresult.show(false)
    tableMD5NUM.show(false)

    println("result.rdd:" + result.rdd.count())
    /*import com.redislabs.provider.redis._
    sc.toRedisLIST(
      result.rdd.map(f => {
        val rowkey = f.getAs[String](0)
        //        rowkey + "|||||o_status"
        "{\"rowkey\":" + "\"" + rowkey + "\"" + "," + "\"table\":\"" + tableNames + "\"}"
      }), "cunzhengxiang:data"
    )*/
  }
}
