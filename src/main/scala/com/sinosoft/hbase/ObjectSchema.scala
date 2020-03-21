package com.sinosoft.hbase

object ObjectSchema {
  /**
    * 公共的schema字符串拼接
    *
    * @param namespace       HBase表空间
    * @param hTable          HBase表名
    * @param rowkey          RowKey，对应spark df的主键字段名，如col0
    * @param tableCoder      hbase表类型(PrimitiveType(就是普通的hbase表) Avro(处理avro格式序列化后的数据时填这个) Phoenix(Phoenix表))
    * @param columnFamily    列簇
    * @param json_label_list 接收一个三元组，二元组里面存放的是spark df的字段名，HBase中的标签名和字段类型，如("col1","name","string")
    * @return schema字符串
    */
  def common_schemas(namespace: String = "default", tableCoder: String = "PrimitiveType")
                    (hTable: String, rowkey: String, columnFamily: String, json_label_list: Tuple3[String, String, String]*): String = {
    val start =
      s"""{
         |   "table":{"namespace":"$namespace", "name":"${hTable}", "tableCoder":"$tableCoder"},
         |   "rowkey":"key",
         |   "columns":{
         |       "${rowkey}":{"cf":"rowkey", "col":"key", "type":"string"}""".stripMargin
    val builder = new StringBuilder
    builder.append(start)
    for (i <- 0 to json_label_list.length - 1) {
      builder.append(",")
      val schema =
        s"""
           |       "${json_label_list(i)._1}":{"cf":"${columnFamily}", "col":"${json_label_list(i)._2}", "type":"${json_label_list(i)._3}"}""".stripMargin
      builder.append(schema)
    }
    builder.append("\n   }\n")
    builder.append("}")
    builder.toString()
  }

  /**
    *
    * @param namespace  HBase表空间
    * @param hbase_base hbase表名
    * @param tableCoder hbase表类型(PrimitiveType(就是普通的hbase表) Avro(处理avro格式序列化后的数据时填这个) Phoenix(Phoenix表))
    * @param rowkey     rowkey对应的spark df字段名，如col0
    * @param json       hbase标签对应的spark df字段名，如col1
    * @param cf         hbase列簇
    * @param lable_name hbase标签名
    * @return
    */
  def common_schema(namespace: String = "default", tableCoder: String = "PrimitiveType")
                   (hbase_base: String, rowkey: String, json: String, cf: String, lable_name: String): String = {
    val schema =
      s"""{
         |   "table":{"namespace":"${namespace}", "name":"${hbase_base}", "tableCoder":"${tableCoder}"},
         |   "rowkey":"key",
         |   "columns":{
         |       "${rowkey}":{"cf":"rowkey", "col":"key", "type":"string"},
         |       "${json}":{"cf":"${cf}", "col":"${lable_name}", "type":"string"}
         |   }
         |}""".stripMargin
    schema
  }

}
