package com.sinosoft

class Worker private{
  def work() = println("I am the only worker!")
}

object Worker{
  val worker = new Worker
  def GetWorkInstance() : Worker = {
    worker.work()
    worker
  }
}

object Jobs{
  def main(args: Array[String]) {
    for (i <- 1 to 5) {
      val work = Worker.GetWorkInstance();
      println(work)
    }
  }
}
