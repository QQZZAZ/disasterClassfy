package com.sinosoft

import com.sinosoft.commen.Hbase.OperationHbase
import com.sinosoft.utils.MyUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.HashSet

object ReadHDFSXJ {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("ReadHdfs")
      //      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    //    val dfa = spark.read.textFile("hdfs://10.10.30.3:9000/zdb/beijing/tb/tb—a")

    analysize(spark, "/test/datamatching_result", "/test/result", "")

    //    weibo(sc)
    //    tieba(sc)
    spark.close()

  }

  def analysize(spark: SparkSession, in: String, out: String, tableName: String) = {
    val dfb = spark.read.parquet("hdfs://10.10.30.3:9000" + in)


    import spark.implicits._
    val result = dfb.map(f => {
      //      val conn = OperationHbase.createHbaseClient()
      //      val map = OperationHbase.QueryByConditionTest(tableName, f, conn)
      //      val json = new JSONObject()
      //      import scala.collection.JavaConversions._
      //      map.foreach(dd => {
      //        json.put(dd._1, dd._2)
      //      })
      //      json.put("rowkey", f)
      //      if (conn != null) {
      //        conn.close()
      //      }
      //      json.toString
      val jo = new JSONObject(f.getAs[String](0))
      val rowkey = jo.getString("rowkey")
      var tableName = jo.getString("table")
      if (tableName.equals("WEIBO_INFO_TABLE")) {
        jo.put("u_id", if (jo.isNull("wb_user_id")) "" else jo.getString("wb_user_id"))
        jo.put("u_url", if (jo.isNull("wb_user_url")) "" else jo.getString("wb_user_url"))
        jo.put("u_nickname", if (jo.isNull("wb_nickname")) "" else jo.getString("wb_nickname"))
        jo.put("r_is_trans", if (jo.isNull("wb_is_trans")) "" else jo.getString("wb_is_trans"))
        jo.put("m_origin_url", if (jo.isNull("wb_origin_id")) "" else jo.getString("wb_origin_id"))
        jo.put("m_origin_rowkey", if (jo.isNull("wb_origin_rowkey")) "" else jo.getString("wb_origin_rowkey"))
        jo.put("m_origin_mid", if (jo.isNull("wb_origin_mid")) "" else jo.getString("wb_origin_mid"))
        jo.put("m_content_id", if (jo.isNull("wb_weibo_id")) "" else jo.getString("wb_weibo_id"))
        jo.put("m_content_url", if (jo.isNull("wb_weibo_url")) "" else jo.getString("wb_weibo_url"))
        jo.put("m_content", if (jo.isNull("wb_content")) "" else jo.getString("wb_content"))
        jo.put("m_images", if (jo.isNull("wb_images")) "" else jo.getString("wb_images"))
        jo.put("m_content_mid", if (jo.isNull("wb_mid")) "" else jo.getString("wb_mid"))
        jo.put("g_spider_time", if (jo.isNull("wb_scraptime")) "" else jo.getString("wb_scraptime"))
        jo.put("r_trans_num", if (jo.isNull("wb_transfer_num")) "" else jo.getString("wb_transfer_num"))
        jo.put("r_comment_num", if (jo.isNull("wb_comment_num")) "" else jo.getString("wb_comment_num"))
        jo.put("r_praised_num", if (jo.isNull("wb_like_num")) "" else jo.getString("wb_like_num"))
        jo.put("g_publish_time", if (jo.isNull("wb_pubtime")) "" else jo.getString("wb_pubtime"))
        jo.put("g_update_time", if (jo.isNull("wb_updatetime")) "" else jo.getString("wb_updatetime"))
        jo.put("m_video_url", if (jo.isNull("wb_video")) "" else jo.getString("wb_video"))
        jo.put("m_video_length", if (jo.isNull("wb_video_length")) "" else jo.getString("wb_video_length"))
        jo.put("m_audio_url", if (jo.isNull("wb_audio")) "" else jo.getString("wb_audio"))
        jo.put("m_tools", if (jo.isNull("wb_tools")) "" else jo.getString("wb_tools"))
        jo.put("u_logo", if (jo.isNull("wb_user_logo")) "" else jo.getString("wb_user_logo"))
        jo.put("m_is_remove", if (jo.isNull("wb_is_remove")) "" else jo.getString("wb_is_remove"))
        jo.put("u_is_remove", if (jo.isNull("wb_user_is_remove")) "" else jo.getString("wb_user_is_remove"))
        jo.put("content_md5", if (jo.isNull("wb_content_md5")) "" else jo.getString("wb_content_md5"))
      } else if (tableName.equals("INFO_TABLE")) {
        jo.put("table", "NEWS_INFO_TABLE")
        //新闻地址
        jo.put("m_content_url", if (jo.isNull("web_info_news_url")) "" else jo.getString("web_info_news_url"))
        //是否是属地新闻
        jo.put("m_is_local", if (jo.isNull("web_info_web_local")) "" else jo.getString("web_info_web_local"))
        //是否是境内新闻
        jo.put("m_is_inner", if (jo.isNull("web_info_web_inner")) "" else jo.getString("web_info_web_inner"))
        //新闻主站URL
        jo.put("m_origin_url", if (jo.isNull("web_info_website_url")) "" else jo.getString("web_info_website_url"))
        //新闻主站名称
        jo.put("m_domain_name", if (jo.isNull("web_info_website_name")) "" else jo.getString("web_info_website_name"))
        //评论数目
        jo.put("r_comment_num", if (jo.isNull("web_info_comments_num")) "" else jo.getString("web_info_comments_num"))
        //评论点赞数
        jo.put("r_respond_num", "")
        //文章点赞数
        jo.put("r_praised_num", if (jo.isNull("web_info_like_num")) "" else jo.getString("web_info_like_num"))
        //文章阅读数
        jo.put("r_read_num", "")
        //新闻栏目
        jo.put("m_catalog_name", "")
        //新闻编辑
        jo.put("m_editor", if (jo.isNull("web_info_editor")) "" else jo.getString("web_info_editor"))
        //新闻正文
        jo.put("m_content", if (jo.isNull("web_info_content")) "" else jo.getString("web_info_content"))
        //新闻的标题
        jo.put("m_title", if (jo.isNull("web_info_title")) "" else jo.getString("web_info_title"))
        //图片地址集
        jo.put("m_images", if (jo.isNull("web_info_images")) "" else jo.getString("web_info_images"))
        //视频地址集
        jo.put("m_videos", if (jo.isNull("web_info_videos")) "" else jo.getString("web_info_videos"))
        //发布时间
        jo.put("g_publish_time", if (jo.isNull("web_info_push_time")) "" else jo.getString("web_info_push_time"))
        //采集时间
        jo.put("g_spider_time", if (jo.isNull("web_info_spider_time")) "" else jo.getString("web_info_spider_time"))
        //新闻域名
        jo.put("m_domain", if (jo.isNull("web_info_domain")) "" else jo.getString("web_info_domain"))
        //新闻源头
        jo.put("m_relation", if (jo.isNull("web_info_news_from")) "" else jo.getString("web_info_news_from"))
        //音频地址集
        jo.put("m_audios", if (jo.isNull("web_info_audio")) "" else jo.getString("web_info_audio"))
        //文章页面渲染地址集
        jo.put("m_render_url", if (jo.isNull("web_info_js_css")) "" else jo.getString("web_info_js_css"))
        //更新时间
        jo.put("g_update_time", if (jo.isNull("web_info_update_time")) "" else jo.getString("web_info_update_time"))
        //历史有效性
        jo.put("m_history_effectiveness", if (jo.isNull("web_info_history_effectiveness")) "" else jo.getString("web_info_history_effectiveness"))
        //数据来源
        jo.put("m_source", "['news','news_rule']")
        //正文关键词
        jo.put("s_content_keywords", if (jo.isNull("web_content_keywords")) "" else jo.getString("web_content_keywords"))
        //标题语种
        jo.put("s_title_lang", if (jo.isNull("web_title_lang")) "" else jo.getString("web_title_lang"))
        //正文语种
        jo.put("s_content_lang", if (jo.isNull("web_content_lang")) "" else jo.getString("web_content_lang"))
        jo.put("content_md5", if (jo.isNull("web_info_content_md5")) "" else jo.getString("web_info_content_md5"))
      } else if (tableName.equals("WECHAT_INFO_TABLE")) {
        //文章地址
        jo.put("m_content_url", if (jo.isNull("wx_article_url")) "" else jo.getString("wx_article_url"))
        //微信昵称
        jo.put("m_nickname", if (jo.isNull("wx_nick_name")) "" else jo.getString("wx_nick_name"))
        //作者
        jo.put("m_editor", "")
        //来源
        jo.put("m_relation", if (jo.isNull("wx_group_url")) "" else jo.getString("wx_group_url"))
        //微信号
        jo.put("m_name", if (jo.isNull("wx_user_name")) "" else jo.getString("wx_user_name"))
        //页面介绍
        jo.put("m_desc", if (jo.isNull("wx_desc")) "" else jo.getString("wx_desc"))
        //标题
        jo.put("m_title", if (jo.isNull("wx_title")) "" else jo.getString("wx_title"))
        //正文
        jo.put("m_content", if (jo.isNull("wx_content")) "" else jo.getString("wx_content"))
        //封面图片
        jo.put("m_cover", if (jo.isNull("wx_cover")) "" else jo.getString("wx_cover"))
        //图片地址集
        jo.put("m_images", if (jo.isNull("wx_images")) "" else jo.getString("wx_images"))
        //视频集
        jo.put("m_videos", if (jo.isNull("wx_videos")) "" else jo.getString("wx_videos"))
        //音频集
        jo.put("m_audios", if (jo.isNull("wx_audios")) "" else jo.getString("wx_audios"))
        //发布时间
        jo.put("g_publish_time", if (jo.isNull("wx_pubtime")) "" else jo.getString("wx_pubtime"))
        //微信号LOGO
        jo.put("m_logo", if (jo.isNull("wx_user_logo")) "" else jo.getString("wx_user_logo"))
        //文章关系
        jo.put("m_relationship_url", if (jo.isNull("wx_group_url")) "" else jo.getString("wx_group_url"))
        //采集时间
        jo.put("g_spider_time", if (jo.isNull("wx_scraptime")) "" else jo.getString("wx_scraptime"))
        //点赞数量
        jo.put("r_praised_num", if (jo.isNull("wx_like_num")) "" else jo.getString("wx_like_num"))
        //评论数目
        jo.put("r_comment_num ", "")
        //评论点赞数
        jo.put("r_respond_num", "")
        //阅读数量
        jo.put("r_read_num", if (jo.isNull("wx_read_num")) "" else jo.getString("wx_read_num"))
        //评论集
        jo.put("r_comments", if (jo.isNull("wx_comments")) "" else jo.getString("wx_comments"))
        //更新时间
        jo.put("g_update_time", if (jo.isNull("wx_updatetime")) "" else jo.getString("wx_updatetime"))
        //正文关键词
        jo.put("s_content_keywords", if (jo.isNull("wx_content_keywords")) "" else jo.getString("wx_content_keywords"))
        //标题语种
        jo.put("s_title_lang", if (jo.isNull("wx_title_lang")) "" else jo.getString("wx_title_lang"))
        //正文语种
        jo.put("s_content_lang", if (jo.isNull("wx_content_lang")) "" else jo.getString("wx_content_lang"))
        //数据来源
        jo.put("m_source", "['wechat', 'wechat_app']")
        jo.put("content_md5", if (jo.isNull("wx_content_md5")) "" else jo.getString("wx_content_md5"))

      } else if (tableName.equals("TB_FLOOR_TABLE")) {
        //贴吧源域名归属
        jo.put("m_domain", if (jo.isNull("tieba_fl_domain")) "" else jo.getString("tieba_fl_domain"))
        //域名名称
        jo.put("m_domain_name", if (jo.isNull("tieba_fl_domain_name")) "" else jo.getString("tieba_fl_domain_name"))
        //贴吧归属－名称
        jo.put("m_name", if (jo.isNull("tieba_fl_from_tb")) "" else jo.getString("tieba_fl_from_tb"))
        //贴吧归属地址rowkey
        jo.put("m_origin_url", if (jo.isNull("tieba_fl_from_tb_url")) "" else jo.getString("tieba_fl_from_tb_url"))
        //是否为回复
        jo.put("r_is_reply", if (jo.isNull("tieba_fl_reply_boo")) "" else jo.getString("tieba_fl_reply_boo"))
        //楼id
        jo.put("m_floor_id", if (jo.isNull("tieba_fl_id")) "" else jo.getString("tieba_fl_id"))
        //楼层数
        jo.put("m_floor_num", if (jo.isNull("tieba_fl_floor_num")) "" else jo.getString("tieba_fl_floor_num"))
        //楼发布人昵称
        jo.put("u_nickname", if (jo.isNull("tieba_fl_author_nickname")) "" else jo.getString("tieba_fl_author_nickname"))
        //楼发布人姓名
        jo.put("u_name", if (jo.isNull("tieba_fl_author")) "" else jo.getString("tieba_fl_author"))
        //楼发布人URL-rowkey
        jo.put("u_url", if (jo.isNull("tieba_fl_author_url")) "" else jo.getString("tieba_fl_author_url"))
        //楼内容
        jo.put("m_content", if (jo.isNull("tieba_fl_content")) "" else jo.getString("tieba_fl_content"))
        //楼内容涉及图片
        jo.put("m_images", if (jo.isNull("tieba_fl_images")) "" else jo.getString("tieba_fl_images"))
        //楼内容涉及音频
        jo.put("m_audios", if (jo.isNull("tieba_fl_audio")) "" else jo.getString("tieba_fl_audio"))
        //楼内容涉及视频
        jo.put("m_videos", if (jo.isNull("tieba_fl_video")) "" else jo.getString("tieba_fl_video"))
        //楼层发布人头像地址
        jo.put("u_logo", if (jo.isNull("tieba_fl_head_images")) "" else jo.getString("tieba_fl_head_images"))
        //采集时间
        jo.put("g_spider_time", if (jo.isNull("tieba_fl_spider_time")) "" else jo.getString("tieba_fl_spider_time"))
        //发布时间
        jo.put("g_publish_time", if (jo.isNull("tieba_fl_publish_time")) "" else jo.getString("tieba_fl_publish_time"))
        //发帖设备
        jo.put("m_tools", if (jo.isNull("all_fl_equip")) "" else jo.getString("all_fl_equip"))
        //回复对应上层id
        jo.put("r_parent_id", if (jo.isNull("tieba_fl_origin_id")) "" else jo.getString("tieba_fl_origin_id"))
        //回复对应顶层id
        jo.put("m_top_id", if (jo.isNull("tieba_fl_top_id")) "" else jo.getString("tieba_fl_top_id"))
        //帖子标题
        jo.put("m_title", if (jo.isNull("tieba_fl_title")) "" else jo.getString("tieba_fl_title"))
        //帖子-URL
        jo.put("m_page_url", if (jo.isNull("tieba_fl_website_url")) "" else jo.getString("tieba_fl_website_url"))
        //帖子－rowkey
        jo.put("r_parent_url", if (jo.isNull("tieba_fl_website_url")) "" else jo.getString("tieba_fl_website_url"))
        jo.put("content_md5", if (jo.isNull("tieba_content_md5")) "" else jo.getString("tieba_content_md5"))
      }
      jo.put("web_diff", "yqfx11")
      (rowkey, jo.toString)
    })
    val num = result.map(_._1).distinct().count()
    println("共计" + num + "条数据")

    result.map(_._2).repartition(400).write
      .text("hdfs://10.10.30.3:9000/zdb/beijing" + out)
  }


}
