package com.moretv.bi.report.medusa.dataAnalytics

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/7/16.
  */



object DataAnalyticsLiveDurationDistribution extends BaseClass {

  private val tableName = "data_analytic_live_duration_distribution"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance
        sqlContext.udf.register("getVersion",getVersion _)
        sqlContext.udf.register("durationTransform", durationTransform _)
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          if (p.deleteOld) {
            val deleteSql = s"delete from $tableName where day = ?"
            util.delete(deleteSql, insertDate)
          }
          val insertSql = s"insert into $tableName(day,duration,play_num,total_num) values(?,?,?,?)"
            DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.LIVE,date).
              filter("event = 'switchchannel' and liveType='live'").
              registerTempTable("log_data")
          sqlContext.sql(
            """
              |select durationTransform(duration,'s') as duration,count(userId) as playNum
              |from log_data
              |group by durationTransform(duration,'s')
            """.stripMargin).registerTempTable("log_play")
//          sqlContext.sql(
//            """
//              |select duration,playNum,count(userId) as playUser
//              |from log_play
//              |group by duration,playNum
//            """.stripMargin).registerTempTable("log_play_distribution")
          sqlContext.sql(
            """
              |select a.duration,a.playNum,b.totalNum
              |from log_play as a
              |join
              |(
              |select sum(playNum) as totalNum
              |from log_play
              |) as b
            """.stripMargin).foreachPartition(partition => {
            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            partition.foreach(rdd => {
              try{
                util1.insert(insertSql, insertDate, rdd.getLong(0), rdd.getLong(1),rdd.getLong(2))
              }catch {
                case e:Exception =>
              }
            })
          })
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

  /**
    * Get the version info
    * @param apkVersion
    * @return
    */
  def getVersion(apkVersion:String): String ={
    if(apkVersion!=null){
      apkVersion.substring(0,1)
    }else{
      "未知"
    }
  }


  /**
    * Transform the duration to special output type
    * @param duration
    * @param output
    */
  def durationTransform(duration:Long, output:String):Long = {
    val minuteScale = 60            // Minute scale
    val hourScale = 3600            // Hour scale
    val quarterHourScale = 60*15    // Quarter of Hour scale
    val outDay = 3600*24            // Day scale
    if(duration != null && duration<=outDay) {
      output match {
        case "s" => duration
        case "m" => duration/minuteScale.toLong
        case "q" => duration/quarterHourScale.toLong
        case "h" => duration/hourScale.toLong
        case _ => duration
      }
    }else if(duration>outDay){
      duration+1
    }else {
      0L
    }
  }
}
