package com.sinosoft.utils

import scala.collection.mutable.{HashMap,LinkedHashMap,ArrayBuffer}

class TransformData {

  def registMysql(mysqlData: LinkedHashMap[String,String]): HashMap[String,ArrayBuffer[Tuple3[String,String,String]]]={

    val map = HashMap[String,ArrayBuffer[Tuple3[String,String,String]]]()
    var str = ""
    mysqlData.foreach(f=>{
      var array = ArrayBuffer[Tuple3[String,String,String]]()
      val list = f._1.split("&&")
      //      println(list.length)
      if(list.length <= 1){
        array += new Tuple3[String,String,String]("",f._2,f._1)
        map += (list(0) -> array)
      }else{
        for(i <- 1 until list.length){
          str = str + "(?=.*?" + list(i) +")"
          if(i == list.length - 1){
            if(map.contains(list(0))){
              //              println(map.get(list(0)))
              map(list(0)).append(new Tuple3[String,String,String](str,f._2,f._1))
              str = ""
            }else{
              array += new Tuple3[String,String,String](str,f._2,f._1)
              map += (list(0) -> array)
              str = ""
            }
          }
        }
      }
    })
    //    println("array: "+array)
    println(map)
    map
  }

}
