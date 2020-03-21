package com.sinosoft.sink

import java.sql.{Connection, DriverManager}
import java.sql.Date
import java.text.SimpleDateFormat

import com.google.common.hash.Hashing
import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.Jedis


class HotWords2MysqlSink extends ForeachWriter[Row]() {
  var conn: Connection = _
  var jedis: Jedis = _

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    conn = DriverManager.getConnection(
      "jdbc:mysql://192.168.129.221:3306/analysis?serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=utf-8",
      "bigdata", "123456")
    conn.setAutoCommit(false)

    jedis = new Jedis("192.168.129.211", 6379)
    //    println(conn)
    true
  }

  override def process(value: Row): Unit = {
    println(System.currentTimeMillis())
    val lines = value.get(2).toString
    val map = lines.split(",").map((_, 1)).groupBy(_._1).mapValues(_.size)
    val taskID = value.get(0).toString
    val state = value.get(1).toString
    val taskTime = value.get(3).toString
    val rowkey = value.get(4).toString
    val flag = isExist("duplicateKey", rowkey, jedis)
    //kafka 去重
    if (true) {
      //热词内容表逻辑
      //更新已有数据,更新之前先查询是否存在这条热词
      val itr = map.iterator
      while (itr.hasNext) {
        val next = itr.next()
        val hotWord = next._1
        val contentSelectSql = "select count(1) from hot_word_content " +
          "where thematic_id='" + taskID + "' " +
          "and task_time=STR_TO_DATE('" + taskTime + "','%Y-%m-%d %H:%i:%s') " +
          "and CONTENT='" + hotWord + "'"
        val cps = conn.prepareStatement(contentSelectSql)
        val crs = cps.executeQuery(contentSelectSql)
        // 返回结果集
        var cindex = 0
        if (crs.next()) {
          cindex = crs.getInt(1)
        }
        val num = next._2
        val updateTime = System.currentTimeMillis() / 1000
        if (cindex > 0) {
          val sql = "update hot_word_content SET " +
            "HOT_NUMBER=HOT_NUMBER+" + num + "," +
            "update_time=FROM_UNIXTIME(" + updateTime + ",'%Y-%m-%d %H:%i:%S') " +
            "where thematic_id='" + taskID + "' " +
            "and task_time=STR_TO_DATE('" + taskTime + "','%Y-%m-%d %H:%i:%s') " +
            "and CONTENT='" + hotWord + "'"
          val ps = conn.prepareStatement(sql)
          ps.execute()
        } else {
          //热词不存在，则为第一次插入数据
          val sql = "insert into hot_word_content " +
            "(THEMATIC_ID,CONTENT,HOT_NUMBER,TASK_TIME,CREATE_TIME,UPDATE_TIME) values (" +
            "'" + taskID + "'," +
            "'" + hotWord + "'," +
            +num + "," +
            "STR_TO_DATE('" + taskTime + "','%Y-%m-%d %H:%i:%s')," +
            "FROM_UNIXTIME(" + updateTime + ",'%Y-%m-%d %H:%i:%S')," +
            "FROM_UNIXTIME(" + updateTime + ",'%Y-%m-%d %H:%i:%S'))"
          val ps = conn.prepareStatement(sql)
          ps.execute()
        }
      }

      //热词主题表内容逻辑
      val select_sql = "select count(1) from hot_word_thematic " +
        "where thematic_id='" + taskID + "' " +
        "and task_time=STR_TO_DATE('" + taskTime + "','%Y-%m-%d %H:%i:%s')"
      // 执行sql语句
      val ps = conn.prepareStatement(select_sql)
      val rs = ps.executeQuery(select_sql)
      // 返回结果集
      var index = 0
      if (rs.next()) {
        index = rs.getInt(1)
      }
      val processTime = System.currentTimeMillis() / 1000
      if (index > 0) {
        //判断否是新任务，如果不是则更新数据，不更新创建时间字段和任务id
        //按照指定的准提ID和时间来更新数据
        val sql1 = "update hot_word_thematic set thematic_state='" + state + "'," +
          "update_time=FROM_UNIXTIME(" + processTime + ",'%Y-%m-%d %H:%i:%S') " +
          "where thematic_id='" + taskID + "' " +
          "and task_time=STR_TO_DATE('" + taskTime + "','%Y-%m-%d %H:%i:%s')"
        val ps = conn.prepareStatement(sql1)
        ps.execute()
      } else {
        //走这个逻辑则是认为是第一次传进来的任务，将创建时间也赋值
        val sql2 = "replace into hot_word_thematic " +
          "(thematic_id,thematic_state,task_time,create_time,update_time) values ('" + taskID + "','" +
          "" + state + "'," +
          "STR_TO_DATE('" + taskTime + "','%Y-%m-%d %H:%i:%s')," +
          "FROM_UNIXTIME(" + processTime + ",'%Y-%m-%d %H:%i:%S')," +
          "FROM_UNIXTIME(" + processTime + ",'%Y-%m-%d %H:%i:%S'))"

        val ps = conn.prepareStatement(sql2)
        ps.execute()
      }
      conn.commit()
    }

  }

  /**
    * 判断keys是否存在于集合where中
    */
  def isExist(where: String, key: String, jedis: Jedis) = {
    var index = hash(key)
    if (index < 0) {
      index = ~index
    }
    val result = jedis.getbit(getRedisKey(where), index)
    if (!result) {
      put(where, key, jedis, index)
      val result = jedis.ttl(getRedisKey(where))
      if (result < 0) {
        jedis.expire(getRedisKey(where), 86400)
      }
    }
    result
  }

  /**
    * 将key存入redis bitmap
    */
  private def put(where: String, key: String, jedis: Jedis, index: Long): Unit = {
    jedis.setbit(getRedisKey(where), index, true)
  }

  /**
    * 获取一个hash值 方法来自guava
    */
  def hash(key: String) = {
    Hashing.murmur3_128.hashString(key).asInt()
  }

  def getRedisKey(where: String) = "yq_hot_number" + where

  override def close(errorOrNull: Throwable): Unit = {
    if (conn != null) {
      conn.close()

    }
    if (jedis != null) {
      jedis.close()
    }
  }
}
