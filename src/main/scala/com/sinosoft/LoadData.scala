package com.sinosoft

import com.sinosoft.hbase.ConnectHbase
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

object LoadData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LoadData")
      //      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val InfoConf = ConnectHbase.hbase_conf("zdbtest")
    val RDD = ConnectHbase.create_Initial_RDD(sc, InfoConf)

    RDD.map{case (_,result) => {
      val rowkey = Bytes.toString(result.getRow)
      val map = result.rawCells().map(x => {
        val key = new String(CellUtil.cloneQualifier(x))
        var value = new String(CellUtil.cloneValue(x))
        (key,value)
      })
      val status = if (result.getValue("wa".getBytes, "o_status".getBytes) != null) Bytes.toString(result.getValue("wa".getBytes, "o_status".getBytes)) else ""

      (rowkey,map,status)
    }}.filter(_._3.equals("8")).map(x => {
      val rowkey = x._1
      val map = x._2
      val jo = new JSONObject()
      jo.put("rowkey",rowkey)
      map.foreach(y => {
        if(y._2 != null){
          jo.put(y._1,y._2)
        }
      })
      jo.toString()
    }).repartition(10).saveAsTextFile("hdfs://10.10.30.3:9000/zdb/beijingfile2")

    spark.close()
    sc.stop()
  }
}
