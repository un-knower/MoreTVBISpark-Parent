package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 统计各个视频源的播放数据
 */
object VideoPlayInfoByDuration extends SparkSetting{
  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)
    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
    val dateTime = "20160716"
    val insertDate = "20160715"
    val medusaDir = s"/log/medusa/parquet/$dateTime/play"
    sqlContext.read.load(medusaDir).registerTempTable("log")
    val infoRdd = sqlContext.sql("select videoSid,userId from log where event ='startplay' and videoSid is not null").
      map(e=>(e.getString(0),e.getString(1)))
    val insertSql = "insert into temp_video_duration_user_play_info(day,videoSid,duration,userId) values(?,?,?,?)"
    infoRdd.collect().foreach(i=>{
      util.insert(insertSql,insertDate,i._1,ProgramDurationUtil.getDurationBySidFromRedis(i._1),i._2)
    })
  }

  /**
   * 从MySQL中提取时长
   * @param sid
   * @return
   */
  def getDurationOfVideo(sid:String):String = {
    val duration = CodeToNameUtils.getProgramDurationFromSid(sid).toLong
    var durationResult = ""
    if(duration>=0 & duration <= 180){
      durationResult = "0~3m"
    }else if(duration>=181 & duration<=600){
      durationResult = "3~10m"
    }else if(duration>=601 & duration<=1800){
      durationResult = "10~30m"
    }else if(duration>=1801 & duration<=3600){
      durationResult = "30~60m"
    }else {
      durationResult = ">60m"
    }
    durationResult
  }

  def getDurationFromSidRedis(sid:String) = {
    val duration = ProgramDurationUtil.getDurationBySidFromRedis(sid)
    var durationResult = ""
    if(duration>=0.toString & duration <= 180.toString){
      durationResult = "0~3m"
    }else if(duration>=181.toString & duration<=600.toString){
      durationResult = "3~10m"
    }else if(duration>=601.toString & duration<=1800.toString){
      durationResult = "10~30m"
    }else if(duration>=1801.toString & duration<=3600.toString){
      durationResult = "30~60m"
    }else {
      durationResult = ">60m"
    }
    durationResult
  }

}
