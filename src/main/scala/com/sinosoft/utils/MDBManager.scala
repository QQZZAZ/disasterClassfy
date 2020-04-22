package com.sinosoft.utils

import java.sql.Connection
import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.sinosoft.config.C3p0PropertiesUtils

/**
  * 连接池方式创建mysql连接
  *
  * @param isLocal
  */
class MDBManager extends Serializable {
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true);
  try {
    cpds.setJdbcUrl(C3p0PropertiesUtils.MYSQL_URL);
    cpds.setDriverClass(C3p0PropertiesUtils.MYSQL_DRIVER);
    cpds.setUser(C3p0PropertiesUtils.MYSQL_USERNAME);
    cpds.setPassword(C3p0PropertiesUtils.MYSQL_PASSWORD);
    cpds.setMaxPoolSize(Integer.valueOf(C3p0PropertiesUtils.MYSQL_MAXPOOLSIZE));
    cpds.setMinPoolSize(Integer.valueOf(C3p0PropertiesUtils.MYSQL_MINPOOLSIZE));
    cpds.setAcquireIncrement(Integer.valueOf(C3p0PropertiesUtils.MYSQL_ACQUIREINCREMENT));
    cpds.setInitialPoolSize(Integer.valueOf(C3p0PropertiesUtils.MYSQL_INITIALPOOLSIZE));
    cpds.setMaxIdleTime(Integer.valueOf(C3p0PropertiesUtils.MYSQL_MAXIDLETIME));
  } catch {
    case ex: Exception => ex.printStackTrace()
  }

  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case ex: Exception => ex.printStackTrace()
        null
    }
  }

  def close(): Unit = {
    try {
      cpds.close()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }
}

/**
  * 多线程并发访问线程池类，每个JVM仅创建一个单例
  */
object MDBManager {
  private var mdbManager: MDBManager = _

  def getMDBManager(): MDBManager = {
    if (mdbManager == null) {
      this.synchronized {
        if (mdbManager == null) {
          mdbManager = new MDBManager
        }
      }
    }
    mdbManager
  }
}