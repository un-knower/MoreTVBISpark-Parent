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



object DataAnalyticsAllPlayDistribution extends BaseClass {

  private val tableName = "data_analytic_all_play_distribution"

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
          val insertSql = s"insert into $tableName(day,version,play_num,play_user,total_user) values(?,?,?,?,?)"
          //临时没有该parquet文件,容错
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW,date).select("userId", "videoSid", "contentType",
            "event", "apkVersion").unionAll(
            DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY,date).select("userId", "videoSid", "contentType",
              "event", "apkVersion")).registerTempTable("log_data")
          sqlContext.sql(
            """
              |select getVersion(apkVersion) as version,userId,videoSid,count(userId) as playNum
              |from log_data
              |where event in ('startplay','playview') and contentType in ('movie','tv','zongyi','comic','kids','hot','sports','mv','xiqu','jilu')
              |and getVersion(apkVersion) in ('2','3')
              |group by getVersion(apkVersion),userId,videoSid
            """.stripMargin).registerTempTable("log_play")
          sqlContext.sql(
            """
              |select version,playNum,count(userId) as playUser
              |from log_play
              |group by version,playNum
            """.stripMargin).registerTempTable("log_play_distribution")
          sqlContext.sql(
            """
              |select a.version,a.playNum,a.playUser,b.totalUser
              |from log_play_distribution as a
              |join
              |(
              |select version,sum(playUser) as totalUser
              |from log_play_distribution
              |group by version
              |) as b
              |on a.version = b.version
            """.stripMargin).foreachPartition(partition => {
            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            partition.foreach(rdd => {
              util1.insert(insertSql, insertDate, rdd.getString(0), rdd.getLong(1), rdd.getLong(2),rdd.getLong(3))
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
