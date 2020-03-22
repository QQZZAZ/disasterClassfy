package com.sinosoft

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

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
    conf.set("hbase.zookeeper.quorum", "")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val sc = spark.sparkContext
    // 模拟一个rdd生成  map是列名和列值  还没有指定列族
    val rdd = sc.parallelize((1 to 500).map(rowkey => {
      rowkey -> Map("column1" -> (rowkey.toString + "column"), "column2" -> (rowkey + "column2"))
    }), 50)

    /*implicit val bytesOrdering = new Ordering[Int] {
      override def compare(a: Int, b: Int) = {
        val ord = Bytes.compareTo(Bytes.toBytes(a), Bytes.toBytes(b))
        // if (ord == 0) throw KeyDuplicatedException(a.toString)
        ord
      }
    }*/

    rdd.repartitionAndSortWithinPartitions(new HFilePartitioner(Array("111112","3213213")))
      .flatMap {
        case (key, columns) =>
          val rowkey = new ImmutableBytesWritable()
          rowkey.set(Bytes.toBytes(key)) //设置rowkey
          val kvs = new java.util.TreeSet[KeyValue](KeyValue.COMPARATOR)
//          val kvs = new mutable.TreeSet[KeyValue](KeyValue.COMPARATOR)

          columns.foreach(ele => {
            val (column, value) = ele // 每一条数据两个列族  对应map里面的两列
            kvs.add(new KeyValue(rowkey.get(), Bytes.toBytes("family1"), Bytes.toBytes(column), Bytes.toBytes(value)))
            kvs.add(new KeyValue(rowkey.get(), Bytes.toBytes("family2"), Bytes.toBytes(column), Bytes.toBytes(value)))
          })
          import scala.collection.JavaConversions._
          kvs.toSeq.map(kv => (rowkey, kv))
      }.saveAsNewAPIHadoopFile("aaaa",
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)
  }

}

class HFilePartitioner(startkeyArr: Array[String]) extends Partitioner {

  override def numPartitions: Int = startkeyArr.length

  override def getPartition(key: Any): Int = {
    val domain = key.asInstanceOf[String]

    for (i <- 0 until startkeyArr.length) {
      if (domain.toString().compare(startkeyArr(i)) < 0) {
        return i - 1
      }
    }
    //default return 1
    return startkeyArr.length - 1
  }

  override def equals(other: Any): Boolean = other match {
    case h: HFilePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}



