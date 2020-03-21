package com.sinosoft

import com.sinosoft.commen.Hbase.HbaseAPI
import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.utils.{MyUtils}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object ScanUsers {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ScanUsers")
      .master("local[*]")
      .getOrCreate();
    val sc = spark.sparkContext
//    val HbaseRDD = HbaseAPI.getHBaseData(sc)
//    sc.addSparkListener(new MyListener)
//    val WBInfoConf = ConnectHbase.hbase_conf("WEIBO_INFO_TABLE")

//    val sc = spark.sparkContext
//    val startTime = MyUtils.timeTransformation("2018-1-1")
//    println(startTime)
    import spark.implicits._
//    val start = sc.broadcast(startTime)

    val list = List(("u01","2017/1/21",5),
      ("u02","2017/1/23",6),
      ("u03","2017/1/22",8),
      ("u04","2017/1/20",3),
      ("u01","2017/1/23",6),
      ("u01","2017/2/21",8),
      ("u02","2017/1/23",6),
      ("u01","2017/2/22",4)
    )
    val dataf = sc.parallelize(list).toDF("userID","mn","actionNum")
    dataf.createOrReplaceTempView("action")
    //按姓名统计每月的访问次数
    //    hive.sql("select t1.userid,\n sum(t1.actionNum)\n from\n(select userid,date_format(regexp_replace(mn,'/','-'),'yyyy-MM') as mn,actionNum from action) t1\ngroup by t1.userid,t1.mn")
    spark.sql("select t1.userid, t1.mn," +
      "sum(t1.actionNum) " +
      "from(select userid,date_format(regexp_replace(mn,'/','-'),'yyyy-MM') as mn," +
      "actionNum from action) t1 " +
      "group by t1.userid,t1.mn").show()

    //当group by后面接多个字段时，无法用partition by 进行代替
    spark.sql("select " +
      "  userid," +
      "  mn," +
      "  sum(actionNum) over(partition by userid order by mn) as sum_num " +
      "from(" +
      "  select " +
      "    t1.userid as userid," +
      "    t1.mn as mn," +
      "    sum(actionNum) as actionNum " +
      "  from(select userid,date_format(regexp_replace(mn,'/','-'),'yyyy-MM') as mn,actionNum from action) t1 " +
      "  group by t1.userid,t1.mn) t2").show()

    sc.stop()
    spark.close()
  }
}
