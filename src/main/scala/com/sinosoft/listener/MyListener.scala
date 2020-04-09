package com.sinosoft.listener

import java.io.PrintWriter

import org.apache.spark.scheduler._
/**
  * 经验证发现监听失败任务的日志，就是时间无法根据步进值打印，例如10秒一打印，恐怕会产生海量日志
  * 不知道这个监听器是多长时间调用一次
  * 产生的日志，存在driver客户端所在的服务器上
  * 目前看来不如爬虫爬取sparkUI界面来的简单
  */
class MyListener extends SparkListener {

  /*override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println(stageSubmitted.stageInfo.numTasks)
    val reason = stageSubmitted.stageInfo.failureReason.getOrElse("")
    println(reason)
    println(stageSubmitted.stageInfo.details)
    //    println(stageSubmitted.stageInfo.)


  }
*/
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
   /* if (taskStart.taskInfo.successful) {
      MyListener.index = MyListener.index + 1
    }
    println("MyListener.index:"+MyListener.index)*/
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
//    println("我进来了"+MyListener.pr)
    if (taskEnd.taskInfo.successful) {
      MyListener.index = MyListener.index + 1
    }
    if(MyListener.index > 1){
      MyListener.pr.write("MyListener.index:"+MyListener.index+"\n")
      println("MyListener.index:"+MyListener.index)
      MyListener.pr.flush()
    }
//    Thread.currentThread().interrupt()
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
                                      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
                                        executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
                                      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }

  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {
  }

  override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {
  }

  override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = { }

  object MyListener{
    var index = 0
    val pr = new PrintWriter("/home/hd/out_log/lof.txt")
  }
}
