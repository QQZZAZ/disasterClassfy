package com.sinosoft

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.json.JSONObject

/**
  * 仅能按照事件发生时之后的24小时pv，uv统计。不能精确到每天0点到晚上24点的计算结果
  */
object StructruedStreamingCountPV {
  /** User-defined data type representing the input events */
  case class Event(sessionId: String, timestamp: Timestamp)

  /**
    * User-defined data type for storing a session information as state in mapGroupsWithState.
    *
    * @param numEvents        total number of events received in the session
    * @param startTimestampMs timestamp of first event received in the session when it started
    * @param endTimestampMs   timestamp of last event received in the session before it expired
    */
  case class SessionInfo(
                          numEvents: Int,
                          startTimestampMs: Long,
                          endTimestampMs: Long) {

    /** Duration of the session, between the first and last events */
    def durationMs: Long = endTimestampMs - startTimestampMs
  }

  /**
    * User-defined data type representing the update information returned by mapGroupsWithState.
    *
    * @param id         Id of the session
    * @param durationMs Duration the session was active, that is, from first event to its expiry
    * @param numEvents  Number of events received by the session while it was active
    * @param expired    Is the session active or expired
    */
  case class SessionUpdate(
                            id: String,
                            durationMs: Long,
                            numEvents: Int,
                            expired: Boolean)

  // scalastyle:on println
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("StructuredSessionization").master("local")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.129.11:9092,192.168.129.12:9092")
      .option("subscribe", "dblab")
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, treat words as sessionId of events
    val events = lines
      .select("value", "timestamp")
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)].map(f => {
      val js = new JSONObject(f._1)
      val value = js.getString("monitor_id")
      Event(value, f._2)
    })
    /*val qu = events.writeStream.option("truncate",false)
      .outputMode("append").format("console")
      .trigger(Trigger.ProcessingTime(
        1000
      )).start()
      qu.awaitTermination()*/

    // Sessionize the events. Track number of events, start and end timestamps of session, and
    // and report session updates.
    val sessionUpdates = events.withWatermark("timestamp","1 minutes")
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout()) {

      case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>
        val timestamps = events.map(_.timestamp.getTime).toSeq
        // If timed out, then remove session and send final update
        if (state.hasTimedOut) {
          val finalUpdate =
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
          state.remove()
          finalUpdate
        } else {
          // Update start and end timestamps in session
          val updatedSession = if (state.exists) {
            val oldSession = state.get
            SessionInfo(
              oldSession.numEvents + timestamps.size,
              oldSession.startTimestampMs,
              math.max(oldSession.endTimestampMs, timestamps.max))
          } else {
            SessionInfo(timestamps.size, timestamps.min, timestamps.max)
          }
          state.update(updatedSession)

          // 事件时间模式下，不需要设置超时时间，会根据Watermark机制自动超时
          // 处理时间模式下，可设置个超时时间，根据超时时间清理状态，避免状态无限增加
          // groupState.setTimeoutDuration(1 * 10 * 1000)

          // Set timeout such that the session will be expired if no data received for 10 seconds

          //可以设置最小和最大时间的 事件时间间隔 之后清除状态 前置必须是GroupStateTimeout.EventTimeTimeout()
//          state.setTimeoutTimestamp(timestamps.min,"1 minutes")
          //初次事件造成之后的30秒后清除状态 前置必须是GroupStateTimeout.ProcessingTimeTimeout()
          //一旦属于初始创建或者更新，则重置30秒的超时时间
          state.setTimeoutDuration("30 seconds")
          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
        }
    }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}


