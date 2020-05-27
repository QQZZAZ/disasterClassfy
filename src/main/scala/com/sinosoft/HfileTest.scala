package com.sinosoft


import com.sinosoft.javas.{HashAl, KeyQualifierComparator}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles

import scala.collection.mutable.ArrayBuffer

/**
  * 重点在repartitionAndSortWithinPartitions算子的使用
  * 分区排序，分区内部key排序
  * 如果不这样做，hbase写入会报错
  * Added a key not lexically larger than previous
  */
object HfileTest {

  case class ResultKV(k: ImmutableBytesWritable, v: KeyValue)

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .setAppName("spark-gen-hfile")
    sparkConf.registerKryoClasses(Array(
      classOf[ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.KeyValue],
      classOf[Array[org.apache.hadoop.hbase.io.ImmutableBytesWritable]],
      Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
      Class.forName("scala.reflect.ClassTag$$anon$1")
    ))

    val spark = SparkSession.builder()
      .appName("HfileTest")
      .master("local[*]")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val tableName = "blog"
    val connection = ConnectionFactory.createConnection(conf)

    val table = connection.getTable(TableName.valueOf("blog"))
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job,
      table,
      connection.getRegionLocator(TableName.valueOf("blog")))


    val arr = new ArrayBuffer[String]()
    for (i <- 50000 to 100000) {
      arr.append(i.toString)
    }

    val sc = spark.sparkContext
    // 模拟一个rdd生成  map是列名和列值  还没有指定列族
    val rdd = sc.parallelize((50000 to 100000).map(rowkey => {
      "0001|"+rowkey -> Map("column1" -> (rowkey.toString + "column"), "column2" -> (rowkey + "column2"))
    }), 50)


    println(rdd.getNumPartitions)

    /**
      * key怎么排序，在这里定义
      * 为什么在这里声明一个隐式变量呢，是因为在源码中，方法中有一个隐式参数；不设置是按照默认的排序规则进行排序
      *
      */
    implicit val my_self_Ordering = new Ordering[String] {
      override def compare(a: String, b: String): Int = {
        val a_b: Array[String] = a.split("|")
        val a_1 = a_b(0).toInt
        //根据key的类型来计算值
        val a_2 = a_b(1).toInt
        val b_b = b.split("_")
        val b_1 = b_b(0).toInt
        //根据key的类型计算值
        val b_2 = b_b(1).toInt
        if (a_1 == b_1) {
          a_2 - b_2
        } else {
          a_1 - b_1
        }
      }
    }

    rdd.repartitionAndSortWithinPartitions(new HFilePartitioner(50))
      .flatMap {
        case (key, columns) =>
          val rowkey = new ImmutableBytesWritable()
          var keyN = HashAl.RSHash(key.toString, 49)
          val hash = HashAl.getHash(key.toString)
          var resultK = ""
          if (keyN.toString.size >= 2) {
            resultK = "00" + keyN + "|" + hash
          } else {
            resultK = "000" + keyN + "|" + hash
          }
          rowkey.set(Bytes.toBytes(resultK)) //设置rowkey

          val kvs = new java.util.TreeSet[KeyValue](KeyValue.COMPARATOR)

          columns.foreach(ele => {
            val (column, value) = ele // 每一条数据两个列族  对应map里面的两列
            kvs.add(new KeyValue(rowkey.get(), Bytes.toBytes("family1"), Bytes.toBytes(column), Bytes.toBytes(value)))
            kvs.add(new KeyValue(rowkey.get(), Bytes.toBytes("family2"), Bytes.toBytes(column), Bytes.toBytes(value)))
          })
          import scala.collection.JavaConversions._
          kvs.toSeq.map(kv => (rowkey, kv))
      }.saveAsNewAPIHadoopFile("D:/Hfile/blog.hfile",
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)

    val admin = connection.getAdmin
    val table2 = connection.getTable(TableName.valueOf("blog"))
    val load = new LoadIncrementalHFiles(conf)
    val start = System.currentTimeMillis()
    load.doBulkLoad(new Path("D:/Hfile/blog.hfile"),
      admin,
      table2,
      connection.getRegionLocator(TableName.valueOf("blog")))

    val end = System.currentTimeMillis()
    println(end - start + "ms")
    spark.close()
    sc.stop()
  }

}

class HFilePartitioner(num: Int) extends Partitioner {

  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val domain = key.toString

    /* for (i <- 0 until startkeyArr.length) {
       if (domain.toString().compare(startkeyArr(i)) < 0) {
         return i - 1
       }
     }*/
    //default return 1
    var n = HashAl.RSHash(domain, num)
    n
  }

  override def equals(other: Any): Boolean = other match {
    case h: HFilePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}




