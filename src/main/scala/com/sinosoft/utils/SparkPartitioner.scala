package com.sinosoft.utils

import org.apache.spark.{HashPartitioner, Partitioner}

class SparkPartitioner(numpar:Int) extends Partitioner{
  override def numPartitions: Int = numpar

  override def getPartition(key: Any): Int = {
    val domain = key.hashCode()
    val code = (domain.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }
  override def equals(other: Any): Boolean = other match {
    case iteblog: SparkPartitioner =>
      iteblog.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

}
