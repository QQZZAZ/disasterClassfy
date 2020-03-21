package com.sinosoft.sink

import java.util.Collections

import com.sinosoft.utils.EnumUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by guo on 2019/6/14.
  * 自定义写Hbase的 sink
  */
class HbaseSink extends ForeachWriter[Row] {
  var configuration: Configuration = _
  var connection: Connection = _
  var table: Table = _

  override def open(partitionId: Long, version: Long): Boolean = {
    configuration = HBaseConfiguration.create
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    configuration.set("hbase.zookeeper.quorum", "192.168.129.204,192.168.129.205,192.168.129.206")
    configuration.set("hbase.master", "192.168.129.204:600000")
    configuration.set("hbase.client.scanner.caching", "1")
    configuration.set("hbase.rpc.timeout", "600000")
    configuration.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000)
    connection = ConnectionFactory.createConnection(configuration)
    true
  }

  override def process(value: Row): Unit = {
    println(value.get(0).toString)
    println(value.get(1).toString)
    println(value.get(3).toString)
    val lines = value.get(2).toString
    val map = lines.split(",").map((_, 1)).groupBy(_._1).mapValues(_.size)
    println(map)


  }

  override def close(errorOrNull: Throwable): Unit = {
    if (table != null) {
      table.close()
    }
    if (null != connection) {
      connection.close()
    }
  }
}
