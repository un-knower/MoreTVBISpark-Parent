package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject

/**
 * Created by Administrator on 2016/5/16.
 * 统计各个视频源的播放数据
 */
object EachSourcePlayInfo extends SparkSetting{
  def main(args: Array[String]) {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = new DBOperationUtils("medusa")
        val dateTime = "20160{716,717,718,719,71*,72*,73*,80*,811,812,813,814,815,816}"
        val insertDate = "20160715~20160815"
        val medusaDir = s"/log/medusa/parquet/$dateTime/playqos"
        sqlContext.read.parquet(medusaDir).select("jsonLog").registerTempTable("log")
        val rdd = sqlContext.sql("select jsonLog from log where jsonLog is not null").map(e=>e.getString(0)).
          flatMap(e=>jsonLogParase(e)).filter(e=>{e!=null & e!=""}).filter(e=>{e._1!=null & e._1!=""}).
          filter(e=>{e._2!=null & e._2!=""}).filter(e=>{e._3!=null & e._3!=""})
        rdd.toDF("userId","contentType","videoSource").registerTempTable("log_data")
        val sourceRdd = sqlContext.sql("select videoSource,count(userId),count(distinct userId) from log_data group by videoSource")
        val sourceContentTypeRdd = sqlContext.sql("select videoSource,contentType,count(userId),count(distinct userId) from log_data group by videoSource,contentType")
        val insertSql1 = "insert into temp_baimao_source_play_info(day,source,vv,uv) values(?,?,?,?)"
        val insertSql2 = "insert into temp_baimao_source_contentType_play_info(day,source,contentType,vv,uv) values(?,?,?,?,?)"
        sourceRdd.collect().foreach(i=>{
          util.insert(insertSql1,insertDate,i.getString(0),new JLong(i.getLong(1)),new JLong(i.getLong(2)))
        })
        sourceContentTypeRdd.collect().foreach(i=>{
          util.insert(insertSql2,insertDate,i.getString(0),i.getString(1),new JLong(i.getLong(2)), new JLong(i.getLong(3)))
        })
  }

  def jsonLogParase(jsonLog:String) ={
    val initTuple = ("","","")
    var result = List(initTuple)
    try{
      val jsonObj = new JSONObject(jsonLog)
      val contentType = jsonObj.optString("contentType")
      val userId = jsonObj.optString("userId")
      val playQosInfo = jsonObj.optJSONArray("playqos")
      (0 until playQosInfo.length()).foreach(i=>{
        val info = playQosInfo.getJSONObject(i)
        val videoSource = info.optString("videoSource")
        val currentInfo = (userId,contentType,videoSource)
        result = result.+:(currentInfo)
      })
    }catch{
      case e:Exception =>
    }
    result
  }
}
