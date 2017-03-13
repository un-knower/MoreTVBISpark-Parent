package com.moretv.bi.report.medusa.dataAnalytics

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/7/16.
  * 统计各个播放量阈值对整体播放UV与VV的影响
  */



object DataAnalyticsPlayThresholdResultDistribution extends BaseClass {

  private val tableName = "data_analytic_play_threshold_result_distribution"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance
        sqlContext.udf.register("getVersion",getVersion _)
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
          val insertSql = s"insert into $tableName(day,content_type,version,play_num,valid_play_user,valid_play_num,total_play_user,total_play_num) values(?,?,?,?,?,?,?,?)"
          //临时没有该parquet文件,容错
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW,date).select("userId", "videoSid", "contentType",
            "event", "apkVersion").unionAll(
            DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY,date).select("userId", "videoSid", "contentType",
              "event", "apkVersion")).registerTempTable("log_data")
          sqlContext.sql(
            """
              |select contentType,getVersion(apkVersion) as version,count(distinct userId) as playUser,count(userId) as playNum
              |from log_data
              |where event in ('startplay','playview') and contentType in ('movie','tv','zongyi','comic','kids','hot','sports','mv','xiqu','jilu')
              |and getVersion(apkVersion) in ('2','3')
              |group by contentType,getVersion(apkVersion)
            """.stripMargin).registerTempTable("log_total_play")
          sqlContext.sql(
            """
              |select contentType,getVersion(apkVersion) as version,userId,videoSid,count(userId) as playNum
              |from log_data
              |where event in ('startplay','playview') and contentType in ('movie','tv','zongyi','comic','kids','hot','sports','mv','xiqu','jilu')
              |and getVersion(apkVersion) in ('2','3')
              |group by contentType,getVersion(apkVersion),userId,videoSid
            """.stripMargin).registerTempTable("log_play")
          val playThreshold = sqlContext.sql(
            """
              |select distinct playNum
              |from log_play
              |order by playNum
            """.stripMargin).map(e=>(e.getLong(0)))


          playThreshold.collect.foreach(e => {
            sqlContext.sql(
              s"""
                |select contentType,version,count(userId) as validPlayUser,sum(playNum) as validPlayNum
                |from log_play
                |where playNum<=${e}
                |group by contentType,version
              """.stripMargin).registerTempTable("log_play_valid")
            sqlContext.sql(
              s"""
                |select a.contentType,a.version,${e} as playNum,a.validPlayUser,a.validPlayNum,
                |b.playUser,b.playNum
                |from log_play_valid as a
                |join log_total_play as b
                |on a.contentType = b.contentType and a.version = b.version
              """.stripMargin).
              map(e=>(e.getString(0),e.getString(1),e.getInt(2),e.getLong(3),e.getLong(4),e.getLong(5),e.getLong(6))).
              collect().
              foreach(r=>{
                util.insert(insertSql,insertDate,r._1,r._2,r._3,r._4,r._5,r._6,r._7)
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
}
