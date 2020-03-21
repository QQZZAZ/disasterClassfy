package com.sinosoft.sink

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Created by guo on 2019/7/12.
  */
class ExecutorHeapMysqlSink extends ForeachWriter[Row]() {
  var conn: Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://192.168.120.87:3306/singleserverheaptable?serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=utf-8",
      "zkr_cj","Aasd#123")
    conn.setAutoCommit(false)
    true
  }

  override def process(value: Row): Unit = {
    val sql = "replace into application_executor_heap_table " +
      "(id,spider_log_send_ip,application_name,time,heap_number) values (?,?,?,?,?)"


    val ps = conn.prepareStatement(sql)
    ps.setString(1, value.getAs[String](0))
    ps.setString(2, value.getAs[String](1))
    ps.setString(3, value.getAs[String](2))
    ps.setLong(4, value.get(3).toString.toLong)
    ps.setDouble(5, value.get(4).toString.toDouble)

    ps.execute()
    conn.commit()
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}
