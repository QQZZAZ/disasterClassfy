package com.sinosoft

import java.util

import groovy.json.JsonException
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.json.JSONArray

import scala.collection.mutable.ArrayBuffer



class HiveUDTFTest extends GenericUDTF {

  override def initialize(args:Array[ObjectInspector]): StructObjectInspector = {
    import scala.collection.JavaConversions._
    var fieldName: util.List[String] = new ArrayBuffer[String]()
    var fieldType: util.List[ObjectInspector] = new ArrayBuffer[ObjectInspector]()
    //解析2个字段，并为这2个对象的输出对象设定字段类型
    fieldName.add("event_name")
    fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldName.add("event_json")
    fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    //设置返回承诺书的名称和类型
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldName,fieldType)
  }

  override def process(args: Array[AnyRef]): Unit = {
    //获取传进来的参数
    val input = args(0).toString
    try {
      //校验
      if(StringUtils.isBlank(input)){

      }else{
        val json = new JSONArray(input)
        for(i <- 0 until(json.length())){
          val results = new Array[String](2)
          //获取事件名称
          results(0) = json.getJSONObject(i).getString("en")
          //获取时间主体
          results(1) = json.getString(i)
          forward(results)
        }
      }
    }catch {
       case e:JsonException => e.printStackTrace()
    }
  }

  override def close(): Unit = {}
}
