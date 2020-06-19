package com.sinosoft

import java.text.SimpleDateFormat
import java.util.Date

import com.sinosoft.hbase.ConnectHbase
import com.sinosoft.utils.MyUtils
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.json.{JSONObject, JSONArray}

/**
  * 建议在hive客户端建好分区表，然后通过spark写入文件然后通过hive的load命令导入数据
  * 因为部分HQL的DML语法，sparkSQL不支持
  *com.sinosoft.Hive_HbaseTest
  *D:\maven\maven-repository\org\datanucleus\datanucleus-api-jdo\3.2.6\datanucleus-api-jdo-3.2.6.jar
  */
object Hive_HbaseTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val hive = SparkSession.builder()
      .appName("Hive_HbaseTest")
      .enableHiveSupport()
      .master("local[*]")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .getOrCreate()
    System.setProperty("HADOOP_USER_NAME", "root");
    //    println("ok")
    hive.sql("show databases").show(false)
    hive.sql("use default")
    val sc = hive.sparkContext

    hive.sql("show tables").show(false)
    import hive.implicits._
    //    val df = hive.sql("select key,wb_scraptime from weibo_info")
    //    df.createOrReplaceTempView("sparkTable")
    //    hive.sql("drop table test")

    /**
      * 建立外部表并向里面入树据
      */
    //    hive.sql("create EXTERNAL table test (id string,time bigint) ROW FORMAT DELIMITED " +
    //      "stored as textfile" +
    //      " LOCATION 'hdfs://192.168.129.204:9000/user/hive/warehouse/external/test'")
    //    hive.sql("insert into test values(1,'Tom')")
    //    hive.sql("insert overwrite table test select * from sparkTable")

    //    hive.sql("select * from test").show()

    //    val list = df.collect()
    //    list.foreach(println)
    /**
      * 创建分区内部表，倒入数据，然后转成外部表，然后追加数据
      */
    //    hive.sql("create table partitionTable (id string,time bigint)" +
    //      " partitioned by (month string)"

    //      /*" LOCATION 'hdfs://192.168.129.204:9000/user/hive/warehouse/external/partitionTable'"*/)
    //    hive.sql("insert overwrite table partitionTable partition(month='2019-10') select * from sparkTable")

    //不允许在这里进行内外部表互相转换，需要在hive客户端进行转换
    //    hive.sql("alter table partitionTable set tblproperties('EXTERNAL'='TRUE')")

    //    hive.sql("DESC formatted partitionTable").show()
    //    hive.sql("insert overwrite table partitionTable partition(month='2019-11') select * from sparkTable")

    // 查询分区表
    //    hive.sql("select * from partitionTable where month='2019-10'").show()

    //创建二级分区表
    //    hive.sql("create table partitionTable (id string,time bigint)" +
    //            " partitioned by (month string,day string)")
    //查询二级分区表
    //    hive.sql("select * from partitionTable where month='2019-10' and day='11'").show()

    //    hive.sql("create table test2 (id string,time bigint) " +
    //            "stored as parquet tblproperties('parquet.compression'='SNAPPY')"
    //            )
    //    hive.sql("insert overwrite table test2 select * from sparkTable")

    //    val start = System.currentTimeMillis()
    //    hive.sql("insert into table test2 select * from sparkTable")
    //    val end = System.currentTimeMillis()

    //    println(end -start)
    //    hive.sql("select count(1) from test").show()
    //为每一条数据制造随机分区
    //    hive.sql ("select cast(rand() * 10 as int) as fileNum from test").show()

    //    hive.sql("select date_format('2019-10-23','MM')").show()

    //时间加1
    //    hive.sql("select date_add('2019-10-23',1)").show()
    //2个时间相减 前面 - 后面
    //    hive.sql("select datediff('2019-10-23','2019-11-01')").show()
    //字符串替换
    //    hive.sql("select regexp_replace('2019/10/23','/','-')").show()


    /* hive.sql("create table dept (name string,deptNum string,sex string) " +
       "stored as parquet tblproperties('parquet.compression'='SNAPPY')"
     )
     hive.sql("insert into table dept values('悟空','A','male')")
     hive.sql("insert into table dept values('琪琪','A','female')")
     hive.sql("insert into table dept values('库林','B','male')")
     hive.sql("insert into table dept values('亚木叉','B','male')")
     hive.sql("insert into table dept values('贝吉塔','C','male')")
     hive.sql("insert into table dept values('铃木','D','female')")*/

    //统计各部门的性别人数
    /*hive.sql("select deptNum," +
      "sum(case sex when 'male' then 1 else 0 end) maleNum," +
      "sum(case sex when 'female' then 1 else 0 end) femaleNum " +
      "from dept " +
      "group by deptNum"
    ).show()*/

//     另一种玩法hive中可以使用，sparksql不兼容
   /*hive.sql("select deptNum," +
      "sum(if(sex='male'),1,0) maleNum," +
      "sum(if(sex='female'),1,0) femaleNum " +
      "from dept " +
      "group by deptNum"
    ).show()*/

    //子查询+collect_set,查找各部门不同性别人员的姓名
    //    "select concat(sex,'_',deptNum),name as sex_deptNum from dept"
    /*hive.sql("select t.sex_deptNum as sdn," +
      "concat_ws('|',collect_set(name)) as name_array " +
      "from " +
      "(select concat(sex,'_',deptNum) as sex_deptNum,name from dept) t " +
      "group by sdn").show()*/


    /**
      * 创建压缩格式为parquet的分区表
      * 注意在hdfs中的存储的文件格式不是parquet文件，但是大小确实是经过压缩的文件，具体原因不明
      * create EXTERNAL table test4 (id string,time bigint)
      * partitioned by (month string)
      * stored as parquet tblproperties('parquet.compression'='SNAPPY')
      */
    //    hive.sql("insert into table test4 select * from sparkTable")

    /**
      * 创建复杂数据类型表，并添加数据 这里还是再hive中创建表结构sparksql不支持array语句，需要再dataframe中创建ArrayType才可以
      * hive读取parquet文件时不支持对
      *
      * @行 ，
      * @集合 ，
      * @map
      * @struct类型进行自定义
      * 行，集合，map和struct类型进行自定义 仅支持TextFile模式、
      */

    //    hive.sql("drop table movieInfo")
    //    hive.sql("create table movieInfo (mname string,dec array<string>) " +
    //      "row format delimited
    // fields terminated by ',' " + //字段按照‘,’分割
    //      "collection items terminated by ',' " + //集合按照‘,'分割
    //                "map keys terminated by ':' "+  //map按照‘:’分割

    //这里的tored as parquet不支持与上面的fileds，map和collection进行组合使用
    //      "stored as parquet tblproperties('parquet.compression'='SNAPPY')"
    //    )

    /**
      * parquet建表语句
      * create table movieinfo2(name string,type ARRAY<STRING>)
      * ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      * STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
      * OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      * LOCATION 'hdfs://192.168.129.204:9000/user/hive/warehouse/external/movieinfo'
      * TBLPROPERTIES ( 'orc.compress'='snappy');
      */
    //利用df创建数据，和临时表写入parquet文件中
    val list = List(
      ("悟空传2", ("动作,动漫,热血")),
      ("少年的你2", ("爱情,剧情,校园")),
      ("哪吒2", ("动漫,热血")),
      ("新白娘子传奇2", ("温馨,爱情,古装"))
    )
   /* val list2 = List(
      ("悟空传2", Array("动作","动漫","热血")),
      ("少年的你2", Array("爱情","剧情","校园")),
      ("哪吒2", Array("动漫","热血")),
      ("新白娘子传奇2", Array("温馨","爱情","古装"))
    )*/

    val schma = StructType(List(StructField("name", StringType), StructField("type", StringType)))
//        val schma = StructType(List(StructField("name", StringType), StructField("type", ArrayType(StringType))))

    val rdd = sc.parallelize(list)
      .map(f => {
        Row(f._1, f._2)
      })
    val dd = hive.createDataFrame(rdd, schma)
    dd.show(false)
    //    dd.createOrReplaceTempView("dd")
//    dd.coalesce(1).write.mode(SaveMode.Append).parquet("/user/hive/warehouse/external/movieinfo")

//    val ddf = hive.read.parquet("/user/hive/warehouse/external/movieinfo").show()

    //字段值使Array转成列
    hive.sql("select name,ttype from movieinfo2 " +
      "lateral view explode(type) table_tmp as ttype").show()

    hive.sql("select t.ttype,count(ttype) num from " +
      "(select name,ttype from movieinfo2 lateral view explode(type) table_tmp as ttype) t " +
      "group by t.ttype").show()
    //开窗函数的使用
    //    hive.sql("create table business (name string,dec string,cost int)")
    //    hive.sql("insert into table business values('Tom','2019-1-2',30)")
    //    hive.sql("insert into table business values('Som','2019-2-2',31)")
    //    hive.sql("insert into table business values('Tim','2019-1-12',35)")
    //    hive.sql("insert into table business values('Lion','2019-3-2',130)")
    //    hive.sql("insert into table business values('Tim','2019-3-2',430)")
    //    hive.sql("insert into table business values('Lion','2019-3-2',230)")
    //    hive.sql("insert into table business values('Som','2019-4-2',301)")
    //    hive.sql("insert into table business values('Som','2019-4-3',320)")
    //    hive.sql("insert into table business values('Som','2019-4-4',130)")
    //    hive.sql("select * from business").show()
    //1.统计总人数
//        hive.sql("select name,count(*) over() as sum from business group by name").show()
    //2.查询每个月都有谁进行过消费
//        hive.sql("select date_format(dec,'yyyy-MM') as date,name from business " +
//          "group by date_format(dec,'yyyy-MM'),name").show()
    //3.利用开窗函数 按月份查询消费人数，进行累加
//        hive.sql("select dec as date,count(name) " +
//          "over(order by(date_format(dec,'yyyy-MM'))) as num from business").show()
    //4.查询顾客的购买明细和购买总额
//        hive.sql("select date_format(dec,'yyyy-MM') as date,name,sum(cost) over() as count_cost from business").show()
    //5.按人进行分类，按每次购买统计每个人的消费金额的总额利用distribute by 代替group by 区内排序+ sort by
//     distribute by +sort by == partitionby + order by（全局排序）
//        hive.sql("select name,dec,cost," +
//          "sum(cost) over(distribute by name sort by dec) as count_cost from business").show()

    //5.指定获取窗口中的行或行数
    /**
      * 不常用：需要写再over函数里面
      *
      * @CURRENT ROW 获取当前行
      * @n 表示几行
      * @n PRECEDING 获取前几行
      * @n FOLLOWING 获取后几行
      * @UNBOUND 从哪行算起
      *          UNBOUND PRECEDING表示从前面的起点
      *          UNBOUND FOLLOWING表示到后面的终点
      *
      *          常用的需要放在over函数前面
      * @lag 指定从当前行往前的第几行开始
      * @lead 指定从当前行往后的第几行结束
      */
    //查询顾客的每一次的上次购买时间
//        hive.sql("select name,dec,cost," +
//          "lag(dec,1,'0') over(distribute by name sort by dec) as count_cost from business").show()

    //查询最近前3行进行累加之后的消费量（不做掌握，仅做了解，使用不多）
//        hive.sql("select name,dec,cost," +
//          "sum(cost) over(rows between 2 PRECEDING and current row) as count_cost from business").show()
    //查询最近后3行进行累加之后的消费量（不做掌握，仅做了解，使用不多）
//        hive.sql("select name,dec,cost," +
//          "sum(cost) over(rows between current row and 2 following) as count_cost from business").show()
    //查询姓名相同的最近后3此消费进行累加之后的和（不做掌握，仅做了解，使用不多）
//        hive.sql("select name,dec,cost," +
//          "sum(cost) over(partition by name rows between current row and 2 following) as count_cost from business").show()
    //抽出来前20%的数据
//        hive.sql("select name,dec,cost," +
//          "ntile(5) over(order by dec) from business").show()
//        hive.sql("select * from (select name,dec,cost," +
//          "ntile(5) over(order by dec) as ntile from business)t " +
//          "where t.ntile =1").show()


    /**
      * 排序开窗函数 在over外使用
      *
      * @RANK ()    1,1,3,4
      * @DENSE_RANK () 1,1,2,3
      * @ROW_NUMBER () 1,2,3,4
      */
    //        hive.sql("create table subject (name string,subject_type string,score int)")
    //        hive.sql("insert into table subject values('Tom','Math',90)")
    //        hive.sql("insert into table subject values('Som','English',91)")
    //        hive.sql("insert into table subject values('Tim','Chinease',95)")
    //        hive.sql("insert into table subject values('Tom','Math',90)")
    //        hive.sql("insert into table subject values('Som','English',70)")
    //        hive.sql("insert into table subject values('Tim','Chinease',70)")
    //        hive.sql("insert into table subject values('Tom','Math',81)")
    //        hive.sql("insert into table subject values('Som','English',80)")
    //        hive.sql("insert into table subject values('Tim','Chinease',70)")
    //        hive.sql("select name,subject_type,score," +
    //          "rank() over(partition by subject_type order by score desc) as rank, " +
    //          "dense_rank() over(partition by subject_type order by score desc) as dense_rank, " +
    //          "row_number() over(partition by subject_type order by score desc) as row_number " +
    //          "from subject").show()


    //复杂练习题
    /**
      * create table action(
      * userID string,
      * mn string,
      * actionNum int
      * )
      *
      *
      */
    //    hive.sql("insert into action values ('u01','2017/1/21',5)")
    //    hive.sql("insert into action values ('u02','2017/1/23',6)")
    //    hive.sql("insert into action values ('u03','2017/1/22',8)")
    //    hive.sql("insert into action values ('u04','2017/1/20',3)")
    //    hive.sql("insert into action values ('u01','2017/1/23',6)")
    //    hive.sql("insert into action values ('u01','2017/2/21',8)")
    //    hive.sql("insert into action values ('u02','2017/1/23',6)")
    //    hive.sql("insert into action values ('u01','2017/2/22',4)")

    //统计每个用户每月的访问次数和每个用户的累计访问次数
    /**
      * u01  2017-01  11  11
      * u01  2017-02  12  23
      * u02  2017-01  12  12
      * u03  2017-01   8   8
      * u04  2017-01   3   3
      */

    //将时间进行转换
    //select userid,mn,date_format(regexp_replace(mn,'/','-'),'yyyy-MM') from action

    /*val list = List(("u01","2017/1/21",5),
      ("u02","2017/1/23",6),
      ("u03","2017/1/22",8),
      ("u04","2017/1/20",3),
      ("u01","2017/1/23",6),
      ("u01","2017/2/21",8),
      ("u02","2017/1/23",6),
      ("u01","2017/2/22",4)
    )

    val dataf = sc.parallelize(list).toDF("userID","mn","actionNum")
    dataf.createOrReplaceTempView("action")
    //按姓名统计每月的访问次数
//    hive.sql("select t1.userid,\n sum(t1.actionNum)\n from\n(select userid,date_format(regexp_replace(mn,'/','-'),'yyyy-MM') as mn,actionNum from action) t1\ngroup by t1.userid,t1.mn")
    hive.sql("select t1.userid, t1.mn," +
      "sum(t1.actionNum) " +
      "from(select userid,date_format(regexp_replace(mn,'/','-'),'yyyy-MM') as mn," +
      "actionNum from action) t1 " +
      "group by t1.userid,t1.mn").show()

    //当group by后面接多个字段时，无法用partition by 进行代替
    hive.sql("select " +
      "  userid," +
      "  mn," +
      "  sum(actionNum) over(partition by userid order by mn) as sum_num " +
      "from(" +
      "  select " +
      "    t1.userid as userid," +
      "    t1.mn as mn," +
      "    sum(actionNum) as actionNum " +
      "  from(select userid,date_format(regexp_replace(mn,'/','-'),'yyyy-MM') as mn,actionNum from action) t1 " +
      "  group by t1.userid,t1.mn) t2").show()
*/

    //自定义UDF函数执行ETL
    /*hive.udf.register("getLine", (input: String) => {
      var result = ""
      val json = new JSONObject(input)
      result = result.concat(json.get("name").toString).concat("\t")
        .concat(json.get("en").toString).concat("\t")
        .concat(json.get("date").toString).concat("\t")

      result
    })

    val jsonlist = List("{'name':'zdb','en':[{'ets':{'kv':'CBC','type':'jd'},'en':'start'}," +
      "{'ets':{'kv':'ICBC','type':'taobao'},'en':'end'}],'date':'2019-11-11'}",
      "{'name':'ldq','en':[{'ets':{'kv':'BC','type':'taobao'},'en':'start'}," +
        "{'ets':{'kv':'SBC','type':'dangdang'},'en':'backend'}],'date':'2019-11-12'}")
    val df = sc.parallelize(jsonlist).toDF("json")
    //ods原始表
    df.createOrReplaceTempView("ods_basetable")*/
    //    df.show(false)


    /*val df2 = hive.sql("select * from (" +
      "select " +
      "split(getline(json),'\t')[0] as name," +
      "split(getline(json),'\t')[1] as ops, " +
      "split(getline(json),'\t')[2] as event_date " +
      "from ods_basetable " +
      "where getline(json) <> ''" +
      ") ")*/
    //    df2.show(false)
    //dwd ETL表
    /* df2.createOrReplaceTempView("dwd_basetable")

     hive.sql("CREATE TEMPORARY FUNCTION UserDefinedUDTF as 'com.sinosoft.HiveUDTFTest'")
     //执行自定义的UDTF的炸裂函数
     hive.sql("select " +
       "name,event_name,event_json,event_date " +
       "from dwd_basetable " +
       "lateral view UserDefinedUDTF(ops) jj as event_name,event_json")
       .show(false)*/

    /**
      * 在hive中创建表 并利用动态分区实时写入数据
      * create table dwd_event(name string,event_name string,event_json string)
      * PARTITIONED BY (event_day string)
      * ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      * STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
      * OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      * LOCATION 'hdfs://192.168.129.204:9000/user/hive/warehouse/external/dwd'
      * TBLPROPERTIES ( 'orc.compress'='snappy');
      */
    //    hive.sql("set hive.exec.dynamic.partition=true")
    //    hive.sql("set hive.exec.dynamic.partition.mode=nostrict")

    //动态分区默认按照最后一个字段进行分区，如果是多个字段，则按照最后几个字段进行分区，一一对应创建表示的分区字段的顺序
    /*hive.sql("insert overwrite table dwd_event PARTITION(event_day) " +
      "select " +
      "name,event_name,event_json,event_date " +
      "from dwd_basetable " +
      "lateral view UserDefinedUDTF(ops) jj as event_name,event_json")*/
    //根据指定分区获取数据
    //    hive.sql("select * from dwd_event where event_day='2019-11-12'").show(false)

    /**
      * 创建分区明细表，根据业务表，有多少张表，就将ETL之后的基础表分解成多少张表
      *
      */
    /*hive.sql("insert overwrite table dwd_event_detail PARTITION(day_date) " +
      "select name," +
      "event_name," +
      "get_json_object(event_json,'$.ets.kv') kv," +
      "get_json_object(event_json,'$.ets.type') type, " +
      "event_day " +
      "from dwd_event")*/
    //    hive.sql("select * from dwd_event_detail").show(false)
//    hive.sql("select cast('1245.011' as decimal(6,2))").show(false)

//    hive.sql("select if(10>10,1,0) num").show(false)

    //重点hive拉链表
    /*val orderInfoHisList = List(
      ("1","待支付","2019-01-01","9999-99-99"),
      ("2","待支付","2019-01-01","9999-99-99"),
      ("3","已支付","2019-01-01","9999-99-99"))

    val orderInfoList = List(
      ("4","待支付","2019-01-02"),
      ("2","已支付","2019-01-02"),
      ("5","已支付","2019-01-02"))

    hive.sql("drop table if exists tmp")
    hive.sql("create table tmp (id string,ordertype string,start string,end string)")

    val orderHisDF = sc.parallelize(orderInfoHisList).toDF("id","ordertype","start","end")
    val orderDF = sc.parallelize(orderInfoList).toDF("id","ordertype","date")

    orderHisDF.createOrReplaceTempView("orderInfoHis")
    orderDF.createOrReplaceTempView("orderInfo")
    //创建中间临时表，临时表不写入文件，仅作为中间表使用，临时表通过历史数据和当天数据进行合成，在内存中进行运算
    hive.sql("select oh.id,oi.id,oh.ordertype,oh.start," +
      "end " +
//      "if(oi.id is null,oh.end,date_add(oi.date,-1)) end " +
      "from orderInfoHis oh left join " +
      "(select * from orderInfo where date='2019-01-02')oi " +
      "on oh.id=oi.id and oh.end='9999-99-99'").show(false)
    //对于join上的数据进行改变end字段值，没有join上的字段依据oi.id位null，不改变end值
    hive.sql("select oh.id,oi.id,oh.ordertype,oh.start," +
      "if(oi.id is null,oh.end,date_add(oi.date,-1)) end " +
      "from orderInfoHis oh left join " +
      "(select * from orderInfo where date='2019-01-02')oi " +
      "on oh.id=oi.id and oh.end='9999-99-99'").show(false)
    //第三步，将当天数据于临时表的数据进行合并
    hive.sql("select id,ordertype,'2019-01-02','9999-99-99' from orderInfo " +
      "union all" +
      "(select oh.id,oh.ordertype,oh.start," +
      "if(oi.id is null,oh.end,date_add(oi.date,-1)) end " +
      "from orderInfoHis oh left join " +
      "(select * from orderInfo where date='2019-01-02')oi " +
      "on oh.id=oi.id and oh.end='9999-99-99')").show(false)
    //第四步 排序
    hive.sql("select * from " +
      "(select id,ordertype,'2019-01-02' start,'9999-99-99' end from orderInfo " +
      "union all" +
      "(select oh.id,oh.ordertype,oh.start," +
      "if(oi.id is null,oh.end,date_add(oi.date,-1)) end " +
      "from orderInfoHis oh left join " +
      "(select * from orderInfo where date='2019-01-02')oi " +
      "on oh.id=oi.id and oh.end='9999-99-99')) his " +
      "order by his.id,his.start").show(false)*/

//    hive.sql("select next_day(sysdate,1) from dual")
    hive.close()
  }
}
