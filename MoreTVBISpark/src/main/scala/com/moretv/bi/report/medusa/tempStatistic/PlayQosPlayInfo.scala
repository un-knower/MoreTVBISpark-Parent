package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject

/**
 * Created by Administrator on 2016/5/16.
 * 统计各个视频源的播放数据
 */
object PlayQosPlayInfo extends SparkSetting{
  private val contentTypeRe = """("contentType"):("tv"|"mv"|"zongyi"|"comic"|"movie"|"xiqu"|"jilu"|"sports"|"kids"|"hot")""".r
  private val userIdRe = """("userId"):("[0-9a-z]*")""".r
  def main(args: Array[String]) {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val dateTime = "20160{716,717,718,719,71*,72*,73*,80*,811,812,813,814,815,816}"
        val insertDate = "20160715~20160815"
        val medusaDir = s"/log/medusa/parquet/$dateTime/playqos"
        sqlContext.read.parquet(medusaDir).select("jsonLog").registerTempTable("log")
        val rdd = sqlContext.sql("select jsonLog from log where jsonLog is not null").map(e=>e.getString(0)).
          map(e=>jsonLogParase(e)).filter(e=>{e!=null & e!=""})
        rdd.toDF("userId","contentType").registerTempTable("log_data")
        val sourceRdd = sqlContext.sql("select count(userId),count(distinct userId) from log_data")
//        val sourceContentTypeRdd = sqlContext.sql("select contentType,count(userId),count(distinct userId) from log_data group by contentType")
        val insertSql1 = "insert into temp_baimao_total_play_info(day,vv,uv) values(?,?,?)"
//        val insertSql2 = "insert into temp_baimao_contentType_total_play_info(day,contentType,vv,uv) values(?,?,?,?)"
        sourceRdd.collect().foreach(i=>{
          util.insert(insertSql1,insertDate,new JLong(i.getLong(0)),new JLong(i.getLong(1)))
        })
//        sourceContentTypeRdd.collect().foreach(i=>{
//          util.insert(insertSql2,insertDate,i.getString(0),new JLong(i.getLong(1)), new JLong(i.getLong(2)))
//        })
  }

  def jsonLogParase(jsonLog:String)={
    try{
      val jsonObj = new JSONObject(jsonLog)
      val contentType = jsonObj.optString("contentType")
      val userId = jsonObj.optString("userId")
      (contentType,userId)
    }catch {
      case e:Exception => {
        var content = ""
        var userId = ""
        contentTypeRe findFirstMatchIn jsonLog match {
          case Some(p) => content = p.group(2)
          case None =>
        }
        userIdRe findFirstMatchIn jsonLog match {
          case Some(p) => userId = p.group(2)
          case None =>
        }
        (content,userId)
      }
    }

  }
}
