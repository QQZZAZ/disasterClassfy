package com.sinosoft

import java.text.DecimalFormat
import java.util
import java.util.{Calendar, Properties}

import com.sinosoft.utils.{MyPartitioner}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    // 1 读取kafka配置信息
    // import utils.Property Util
    val brokers = "192.168.129.204:9092,192.168.129.205:9092,192.168.129.206:9092"
    //    val brokers = "10.20.30.10:9092,10.20.30.11:9092"

    val topic = "test"
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //    props.put("partitioner.class", classOf[MyPartitioner].getName)
    props.put("producer.type", "sync")
    props.put("batch.num.messages", "1")
    props.put("queue.buffering.max.messages", "1000000")
    props.put("queue.enqueue.timeout.ms", "20000000")
    props.put("kafka.topics", topic)
    //    import java.util.concurrent.Executors
    //    val threadPool = Executors.newFixedThreadPool(1)

    //    threadPool.execute(new ThreadDemo(props))
    val producer = new KafkaProducer[String, String](props)
    producer.send(new ProducerRecord[String, String](props.getProperty("kafka.topics"), "{'infoRowkey':'1001256745-Ie4ataKhK','infoTableName':'WEIBO_INFO_TABLE'}"))

    producer.flush()
    producer.close()

  }


}
