package com.moretv.bi.report.medusa.channeAndPrograma.collecthistory

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 11/28/16.
  */
object CollectTabPlayStat extends BaseClass {

  private val tableName = "collect_intervals_tab_play_stat"

  private val fields = "interval_type,intervals,tab,vv,uv,duration"

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

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.PLAYVIEW,loadDate)
            .filter(s"date between '$sqlDate' and '$sqlDate'")
            .filter("pathMain like '%collect%'")
            .select("pathMain", "userId", "event", "duration")
            .registerTempTable("log_data")

          sqlContext.sql(
            """
              |select case when pathMain like '%收藏追看%' then '收藏追看'
              | when pathMain like '%标签订阅%' then '标签订阅'
              | when pathMain like '%节目预约%' then '节目预约'
              | when pathMain like '%明星关注%' then '明星关注'
              | when pathMain like '%专题收藏%' then '专题收藏'
              | else '其它' end as tab, userId, event, duration
              | from log_data
            """.stripMargin)
            .registerTempTable("log_tmp")

          val dfUser = sqlContext.sql(
            """
              |select tab, count(userId) as vv, count(distinct userId) as uv
              |from log_tmp
              |where event in ('startplay','playview')
              |group by tab
            """.stripMargin)

          val dfDuration = sqlContext.sql(
            """
              |select tab, sum(duration) as duration
              |from log_tmp
              |where event not in ('startplay','playview')
              | and duration between 1 and 10800
              |group by tab
            """.stripMargin)

          dfUser.join(dfDuration, "tab")
            .registerTempTable("log_data")


          if (p.deleteOld) {
            util.delete(deleteSql, "day", sqlDate)
          }

          sqlContext.sql(
            """
              | select tab, vv ,uv , duration / uv
              | from log_data
            """.stripMargin)
            .collect.foreach(e => {

            val tab = e.getString(0)
            val vv = new JLong(e.getLong(1))
            val uv = new JLong(e.getLong(2))
            val duration = new JDouble(e.getDouble(3))
            util.insert(insertSql, "day", sqlDate, tab, vv, uv, duration)
          })
        })
      }
      case None => {

      }
    }
  }


}
