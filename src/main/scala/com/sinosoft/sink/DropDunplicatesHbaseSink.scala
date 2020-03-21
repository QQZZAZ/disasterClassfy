package com.sinosoft.sink

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, TableName}
import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by guo on 2019/6/14.
  * 自定义写Hbase的 sink
  */
class DropDunplicatesHbaseSink extends ForeachWriter[Row] {
  var COUNT_INFO_TABLE: Table = _
  //  val USER_URL_TABLE = "USER_URL_TABLE"
  val USER_URL_TABLE = "zdbtest1"
  var configuration: Configuration = _
  var connection: Connection = _
  var table: Table = _
  var userTable: Table = _
  var tableName: String = _
  var pubTimeEach: String = _
  var userNameEach: String = _
  var jedis: Jedis = _


  override def open(partitionId: Long, version: Long): Boolean = {
    configuration = HBaseConfiguration.create
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    configuration.set("hbase.zookeeper.quorum", "192.168.129.10,192.168.129.11,192.168.129.12")
    configuration.set("hbase.master", "192.168.129.10:600000")
    configuration.set("hbase.client.scanner.caching", "1")
    configuration.set("hbase.rpc.timeout", "600000")
    configuration.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000)
    connection = ConnectionFactory.createConnection(configuration)
    jedis = new Jedis("192.168.129.14", 6379, 6000)

    true
  }

  override def process(value: Row): Unit = {
    if (value.get(0) != null) {
      try {
        value.getAs[String](2) match {
          case "INFO_TABLE" => {
            tableName = "INFO_TABLE"
            pubTimeEach = "web_info_push_time"
            userNameEach = "web_info_website_url"
          }
          case "MIN_VIDEO_INFO_TABLE" => {
            tableName = "MIN_VIDEO_INFO_TABLE"
            pubTimeEach = "pubtime"
            userNameEach = "user_id"
          }
          case "WECHAT_INFO_TABLE" => {
            tableName = "WECHAT_INFO_TABLE"
            pubTimeEach = "wx_pubtime"
            userNameEach = "wx_user_name"
          }
        }
        //        println(value.getAs[String](0) + "====" +tableName)
        table = connection.getTable(TableName.valueOf(tableName))
        val get = new Get(Bytes.toBytes(value.getAs[String](0)))
        val result = table.get(get)
        val map = new mutable.HashMap[String, String]()
        val userCell = result.getColumnLatestCell(Bytes.toBytes("wa"), Bytes.toBytes(userNameEach))
        userTable = connection.getTable(TableName.valueOf(USER_URL_TABLE))
        //获取username值
        val key = new String(CellUtil.cloneValue(userCell))

        val getUser = new Get(Bytes.toBytes(key))
        val Uresult = userTable.get(getUser)
        //判断接口是否包含此username
        if (!Uresult.isEmpty) {
          //如果包含，则获取发布时间，并获取对应的年月 和日
          val timeCell = result.getColumnLatestCell(Bytes.toBytes("wa"), Bytes.toBytes(pubTimeEach))
          val pubTime = new String(CellUtil.cloneValue(timeCell))
          var dateYM, dateD = ""
          if (pubTime != null && !pubTime.equals("")) {
            val tmp = pubTime.split(" ")(0)
            if (tmp.contains("-")) {
              val split = tmp.split("-")
              if (split.length == 3) {
                //切分出年月日
                dateYM = split(0) + "_" + split(1)
                dateD = split(2)
              }
            }
          }

          //todo 获取统计表中的数据 然后 加一
          COUNT_INFO_TABLE = connection.getTable(TableName.valueOf("COUNT_INFO_TABLE"))
          val getUser2 = new Get(Bytes.toBytes(key + "_" + dateYM))
          val Uresult2 = COUNT_INFO_TABLE.get(getUser2)
          var put: Put = new Put(Bytes.toBytes(key + "_" + dateYM))
          var real_time_count = 0
          var count_all = 0
          if (!Uresult2.isEmpty) {
            val real_time_count_cell = Uresult2.getColumnLatestCell(Bytes.toBytes("wa"), Bytes.toBytes("count_real_time_" + dateD))
            if (real_time_count_cell == null) {
              real_time_count = 1
            } else {
              val real_time_count_Test = new String(CellUtil.cloneValue(real_time_count_cell), "utf-8")
              real_time_count = real_time_count_Test.toInt + 1
            }
            val count_all_cell = Uresult2.getColumnLatestCell(Bytes.toBytes("wa"), Bytes.toBytes("count_all_" + dateD))
            if (count_all_cell == null) {
              count_all = 1
            } else {
              count_all = new String(CellUtil.cloneValue(count_all_cell), "utf-8").toInt
              count_all = count_all + 1
            }
            //如果表中含有此key 则更新
            put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("count_real_time_" + dateD), Bytes.toBytes(real_time_count + ""))
            put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("count_all_" + dateD), Bytes.toBytes(count_all + ""))
          } else {
            //如果表中不包含此key，则赋值1
            put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("count_real_time_" + dateD), Bytes.toBytes("1"))
            put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("count_all_" + dateD), Bytes.toBytes("1"))
          }
//          println(value.getAs[String](0))
          jedis.lpush("zdbtest", key + "_" + dateYM)
          jedis.lpush("keylist",value.getAs[String](0))
          COUNT_INFO_TABLE.put(put)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (table != null) {
      table.close()
    }
    if (userTable != null) {
      userTable.close()
    }
    if (COUNT_INFO_TABLE != null) {
      COUNT_INFO_TABLE.close()
    }
    if (null != connection) {
      connection.close()
    }
    if (jedis != null) {
      jedis.close()
    }
  }
}
