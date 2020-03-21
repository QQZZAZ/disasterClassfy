package com.sinosoft.utils

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils

/**
  * 自定义一个kafka的partition
  */
class MyPartitioner extends Partitioner{
  /**
    *
    * @param s topic
    * @param o key
    * @param bytes key的值
    * @param o1 value
    * @param bytes1 value的值
    * @param cluster 节点
    * @return partion的值
    */
  override def partition(s: String, o: Any, bytes: Array[Byte], o1: Any, bytes1: Array[Byte], cluster: Cluster): Int = {
    val partitions = cluster.partitionsForTopic(s)
    val numPartitions = partitions.size
    if(bytes == null) {throw new Exception("key cannot be null..")}
    else{
      if(o.toString.equals("1")){
        return 1
      }else{
        return (Math.abs(Utils.murmur2(bytes)) % (numPartitions))
      }
    }
  }

  override def close(): Unit = {}

  override def configure(map: util.Map[String, _]): Unit = {}
}
