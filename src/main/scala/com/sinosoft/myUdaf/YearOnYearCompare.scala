package com.sinosoft.myUdaf

import java.sql.Timestamp
import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}

case class DateRange(startDate: Timestamp, endDate: Timestamp) {
  def in(targetDate: Date): Boolean = {
    targetDate.before(endDate) && targetDate.after(startDate)
  }
  override def toString(): String = {
    startDate.toLocaleString() + " " + endDate.toLocaleString();
  }
}

class YearOnYearCompare(current: DateRange) extends UserDefinedAggregateFunction {
  val previous: DateRange = DateRange(subtractOneYear(current.startDate), subtractOneYear(current.endDate))
  println(current)
  println(previous)
  //UDAF与DataFrame列有关的输入样式,StructField的名字并没有特别要求，完全可以认为是两个内部结构的列名占位符。
  //至于UDAF具体要操作DataFrame的哪个列，取决于调用者，但前提是数据类型必须符合事先的设置，如这里的DoubleType与DateType类型
  def inputSchema: StructType = {
    StructType(StructField("metric", DoubleType) :: StructField("timeCategory", DateType) :: Nil)
  }
  //定义存储聚合运算时产生的中间数据结果的Schema
  def bufferSchema: StructType = {
    StructType(StructField("sumOfCurrent", DoubleType) :: Nil)
  }
  //标明了UDAF函数的返回值类型
  def dataType: org.apache.spark.sql.types.DataType = DoubleType

  //用以标记针对给定的一组输入,UDAF是否总是生成相同的结果
  def deterministic: Boolean = true

  //对聚合运算中间结果的初始化
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
//    buffer.update(1, 0.0)
  }

  //第二个参数input: Row对应的并非DataFrame的行,而是被inputSchema投影了的行。以本例而言，每一个input就应该只有两个Field的值
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0)
//    buffer(1) = buffer.getAs[Double](0) + input.getAs[Double](0)
    if (current.in(input.getAs[Date](1))) {
      buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0)
    }

    /*if (current.in(input.getAs[Date](1))) {
      buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0)
    }
    if (previous.in(input.getAs[Date](1))) {
      buffer(1) = buffer.getAs[Double](0) + input.getAs[Double](0)
    }*/
  }

  //负责合并两个聚合运算的buffer，再将其存储到MutableAggregationBuffer中
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
//    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  //完成对聚合Buffer值的运算,得到最后的结果
  def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
    /*if (buffer.getDouble(1) == 0.0) {
      0.0
    } else {
      println((buffer.getDouble(0) - buffer.getDouble(1)) / buffer.getDouble(1) * 100)
      (buffer.getDouble(0) - buffer.getDouble(1)) / buffer.getDouble(1) * 100
    }*/
  }

  private def subtractOneYear(date: Timestamp): Timestamp = {
    val prev = new Timestamp(date.getTime)
    prev.setYear(prev.getYear - 1)
    prev
  }
}
