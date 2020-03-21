package com.sinosoft

import java.io._

import com.sinosoft.commen.Hbase.OperationHbase
import com.sinosoft.utils.GetPlaceWeb
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.io.Source

object LoadUserName2Hbase {
  def main(args: Array[String]): Unit = {
    import scala.collection.JavaConverters._
    val listWebAll: mutable.Buffer[String] = GetPlaceWeb.queryWebAll("web").asScala
    val listWeChatAll: mutable.Buffer[String] = GetPlaceWeb.queryWebAll("WeChat").asScala
    val listDouYinAll: mutable.Buffer[String] = GetPlaceWeb.queryWebAll("douyin").asScala

    /*val jedis = new Jedis("localhost",6379)
    for(wbrurl <- listWebAll){
      jedis.hset(wbrurl,"duplicateKey",wbrurl)
    }
    for(wbrurl <- listWeChatAll){
      jedis.hset(wbrurl,"duplicateKey",wbrurl)
    }
    for(wbrurl <- listDouYinAll){
      jedis.hset(wbrurl,"duplicateKey",wbrurl)
    }
    jedis.close()
    println("over")*/

    val pw = new PrintWriter("/home/hd/out_log/uerUrl.csv")
    for(wbrurl <- listWebAll){
      pw.write(wbrurl)
      pw.write("\n")
    }
    for(wbrurl <- listWeChatAll){
      pw.write(wbrurl)
      pw.write("\n")
    }
    for(wbrurl <- listDouYinAll){
      pw.write(wbrurl)
      pw.write("\n")
    }
    pw.close()
    println("over")
    loadata2Hbase
//    scalaReadFile
  }

  def loadata2Hbase(): Unit = {
    val bufferReader = new BufferedReader(new InputStreamReader
    (new FileInputStream(
      new File("/home/hd/out_log/uerUrl.csv")), "utf-8"))
    val con = OperationHbase.createHbaseClient()
    val table = con.getTable(TableName.valueOf("zdbtest1"))
    var str = bufferReader.readLine()
    try{

      while (str != null) {
        val put = new Put(Bytes.toBytes(str))
        put.addColumn(Bytes.toBytes("wa"), Bytes.toBytes("test"), Bytes.toBytes(""))
        table.put(put)
//        println(str)
        str = bufferReader.readLine()
      }

    }catch {
      case e:Exception => println("invalid key :"+str)
    }finally {
      bufferReader.close()
      table.close()
      con.close()
    }



    println("over")
  }

  def scalaReadFile(): Unit = {
    val file = Source.fromFile("C:\\Users\\zdb\\Desktop\\uerUrl.csv")
    var str = ""
  val arr = for {
    line <- file.getLines
  } yield line

  while(arr.hasNext){
    val ss =arr.next()
    Thread.sleep(500)
    println(ss)
  }

  file.close
}
}
