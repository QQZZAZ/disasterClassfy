package com.sinosoft

import com.sinosoft.CreateTableWeek.tjkafkaurl
import com.sinosoft.myUdaf.MyUdaf
import com.sinosoft.sink.DropDunplicatesHbaseSink
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object LanguegeAnalysis {
  val letters = Map[String, String]("arb" -> "عحقېومۇاۆڭيخغشئءۈتچگژەسرھۋجزـدنفلكپبى",
    "lat" -> "K’flRisLPgbZwxXpëWIYUşShjaÜJvGöetzÖoFQBMqrDHcüyĞğATEËİOmudıŞnVkN'C",
    "cyr" -> "ӘамҚхИПЬөдШгщЛюҲһбЖүХЕшЗзҢыДяеәуғҮЁлнсГЩАЦНҒкІЭҰУБСЫёцЯұвўҳжиКТъФЧчьЮйэӨтРіорОфЪМпВқңЙЎҺ")
  val punc = "[’!\"#\\$\\%\\&\\'\\(\\)\\*\\+\\,\\-\\.\u0014\\<\\=\\>\\?" +
    "\\@\\[\\]\\^\\_\\`\\{|\\}\\~ 。，、＇：∶；?‘’“”〝〞ˆˇ﹕︰﹔﹖﹑·¨….¸;！´？！～—ˉ｜\\‖＂〃｀@﹫¡¿﹏﹋﹌︴" +
    "\\々\\﹟\\#\\﹩\\$\\﹠\\&\\﹪\\%\\*\\﹡\\﹢\\﹦\\﹤‐￣¯―﹨ˆ˜﹍﹎\\+\\=\\<\u00AD\u00AD＿_" +
    "\\〈\\〉\\‹\\›\\﹛\\﹜\\『\\』\\〖\\〗\\［\\］\\《\\》\\〔\\〕\\{\\}\\「\\」\\【\\】" +
    "︵︷︿︹︽_﹁﹃︻︶︸﹀︺︾ˉ﹂﹄︼\\（\\）]+"

  val zh_patteren = "[\u4e00-\u9fa5]"
  val ko_patteren = "[\uAC00-\uD7AF]"
  val jp_patteren = "[\u3040-\u31FF]"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("LanguegeAnalysis")
      .master("local[*]")
      .getOrCreate()
    //    spark.udf.register("u_avg", MyUdaf)
    import spark.implicits._
    val map = new mutable.HashMap[String, mutable.ArrayBuffer[mutable.HashMap[String, mutable.HashSet[String]]]]()
    val staticdf_kz = scalaReadFile("C:/Users/zdb/Desktop/arb/kz.txt")
    val staticdf_ug = scalaReadFile("C:/Users/zdb/Desktop/arb/ug-CN.txt")
    val uz_Cyrl = scalaReadFile("C:/Users/zdb/Desktop/cyr/uz-Cyrl.txt")
    val ug_Cyrl = scalaReadFile("C:/Users/zdb/Desktop/cyr/ug-Cyrl.txt")
    val kk = scalaReadFile("C:/Users/zdb/Desktop/cyr/kk.txt")
    val en_US = scalaReadFile("C:/Users/zdb/Desktop/lat/en-US.txt")
    val tr = scalaReadFile("C:/Users/zdb/Desktop/lat/tr.txt")
    val ug_Latin = scalaReadFile("C:/Users/zdb/Desktop/lat/ug-Latin.txt")
    val ug_Latin_CN = scalaReadFile("C:/Users/zdb/Desktop/lat/ug-Latin-CN.txt")
    val uz_Latin = scalaReadFile("C:/Users/zdb/Desktop/lat/uz-Latin.txt")
    val arblist = new ArrayBuffer[mutable.HashMap[String, mutable.HashSet[String]]]()
    arblist.append(staticdf_kz)
    arblist.append(staticdf_ug)
    val cyrlist = new ArrayBuffer[mutable.HashMap[String, mutable.HashSet[String]]]()
    cyrlist.append(uz_Cyrl)
    cyrlist.append(ug_Cyrl)
    cyrlist.append(kk)
    val latlist = new ArrayBuffer[mutable.HashMap[String, mutable.HashSet[String]]]()
    latlist.append(en_US)
    latlist.append(tr)
    latlist.append(ug_Latin)
    latlist.append(uz_Latin)
    latlist.append(ug_Latin_CN)

    map.put("arb", arblist)
    map.put("cyr", cyrlist)
    map.put("lat", latlist)

    val bromap = spark.sparkContext.broadcast(map)

    val filedf = spark.readStream.textFile("C:\\Users\\zdb\\Desktop\\str")
    val newFiledf = filedf.map(f => {
          def getLang(content: String) = {
            var language = ""
            //todo 先判断是不是数字，标点符号 或者就是空格，或者就是未知的符号
            if (content.replaceAll(punc, "").size == 0) {
              language = "punc"
            } else if (content.replaceAll("[0-9]+", "").size == 0) {
              language = "num"
            } else if (content.replaceAll(punc, "").replaceAll("[0-9]+", "").size == 0) {
              language = "mix"
            } else {
              var index0, index1, index2, index3, index4, index5 = 0
              val set = new mutable.HashMap[String, Int]()
              val arr = content.toCharArray.map(_.toString).toSet
              for (flag <- arr) {
                if (letters.getOrElse("arb", "").contains(flag)) {
                  index0 += 1
                }
                if (letters.getOrElse("lat", "").contains(flag)) {
                  index1 += 1
                }
                if (letters.getOrElse("cyr", "").contains(flag)) {
                  index2 += 1
                }
                if (flag.matches(zh_patteren)) {
                  index3 += 1
                }
                if (flag.matches(ko_patteren)) {
                  index4 += 1
                }
                if (flag.matches(jp_patteren)) {
                  index5 += 1
                }
              }
              if (index0 > 0)
                set.put("arb", index0)
              if (index1 > 0)
                set.put("lat", index1)
              if (index2 > 0)
                set.put("cyr", index2)
              if (index3 > 0)
                set.put("zh-CN", index3)
              if (index4 > 0)
                set.put("ko", index4)
              if (index5 > 0)
                set.put("jp", index5)
              var result = ""
              //        val otherSet = new mutable.HashMap[String, Int]()
              if (set.size >= 1) {
                if (set.size > 1) {
                  result = set.toSeq.sortWith(_._2 > _._2).head._1
                } else {
                  result = set.head._1
                }
                val arrContent = content
                  .replaceAll(punc, " ")
                  .replaceAll("[0-9]+", " ")
                  .split(" ")
                val resultSet = new mutable.HashMap[String, Int]()
                if (result.equals("arb") || result.equals("lat") || result.equals("cyr")) {
                  val wordsMapList = bromap.value.get(result).get
                  for (wordsMap <- wordsMapList) {
                    for ((stype, words) <- wordsMap) {
                      for (word <- arrContent) {
                        if (words.contains(word.toLowerCase)) {
                          //                    println(word)
                          if (resultSet.contains(stype)) {
                            val num = resultSet.get(stype).get + 1
                            resultSet.put(stype, num)
                          } else {
                            resultSet.put(stype, 1)
                          }
                        }
                      }
                    }
                  }
                  if (resultSet.size > 1) {
                    language = resultSet.toSeq.sortWith(_._2 > _._2).head._1
                  } else if (resultSet.size == 1) {
                    language = resultSet.head._1
                  } else {
                    language = "Unknown"
                  }
                } else if (set.get("jp") != None && set.get("jp").get > 0) {
                  if (set.get("jp").get.toFloat / arr.size > 0.1) {
                    language = "jp"
                  }
                } else {
                  language = result
                }
              } else {
                language = "Unknown"
              }
            }
            language
          }
          val language = getLang(f)
          (language, f)
        }).filter(!_.equals("")).toDF("type", "content")
        //自定义UDAF函数但是不适合这个场景 ，需要append模式
        //    val ss = spark.sql("select u_avg(k.lan,n.content) from kz_ugtable k join newFiledf n on k.ftype = n.type")

        val qa = newFiledf.writeStream.option("truncate", false)
          .outputMode("append")
          .format("console")
          .start()
        qa.awaitTermination()
        spark.close()
      }

      def scalaReadFile(path: String) = {
        val file = Source.fromFile(path)
        val index = path.lastIndexOf("/")
        val name = path.substring(index + 1, path.length).split("\\.")(0)
        val map = new scala.collection.mutable.HashMap[String, mutable.HashSet[String]]()
        val arr = for {
          line <- file.getLines
        } yield line

        val set = new mutable.HashSet[String]()
        while (arr.hasNext) {
          val value = arr.next()
          set.add(value)
        }
        map.put(name, set)
        file.close
        map
      }
    }
