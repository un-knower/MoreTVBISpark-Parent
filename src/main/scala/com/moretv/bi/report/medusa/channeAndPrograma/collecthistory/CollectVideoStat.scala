package com.moretv.bi.report.medusa.channeAndPrograma.collecthistory

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.dbOperation.ProgramRedisUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 11/28/16.
  */

/**
  * 每日新增收藏节目统计
  *
  */
object CollectVideoStat extends BaseClass {

  private val tableName = "collect_intervals_video_stat"

  private val fields = "interval_type,intervals,videoSid,videoName,pv,uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where interval_type = ? and intervals = ? "


  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        // init & util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(e => {
          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.COLLECT,loadDate)
            .filter(s"date between '$sqlDate' and '$sqlDate'")
            .filter("collectClass ='video'")
            .select("collectContent", "userId")
            .registerTempTable("log_data")

          val df = sqlContext.sql(
            """
              |select collectContent, count(userId) as pv, count(distinct userId) as uv
              |from log_data
              |group by collectContent
            """.stripMargin)

          if (p.deleteOld) {
            util.delete(deleteSql, "day", sqlDate)
          }

          df.collect.foreach(e => {

            val videoSid = e.getString(0)
            val videoName = ProgramRedisUtil.getTitleBySid(videoSid)
            val pv = new JLong(e.getLong(1))
            val uv = new JLong(e.getLong(2))

            util.insert(insertSql, "day", sqlDate, videoSid, videoName, pv, uv)
          })

        })

      }
      case None => {

      }
    }

  }
}
