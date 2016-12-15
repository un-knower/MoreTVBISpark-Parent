package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.pageStatistics.LauncherViewStatistic._
import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext

/**
 * Created by Administrator on 2016/4/16.
 */
object LauncherClickBasedOnTimeStatistic extends BaseClass{
  def main (args: Array[String]) {
    ModuleClass.executor(LauncherClickBasedOnTimeStatistic,args)
  }
  /*基于时间统计*/
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK

    val restrict = Array(MedusaLogInfoUtil.SEARCHSET_AC,MedusaLogInfoUtil.MYTV_AC,MedusaLogInfoUtil.RECOMMENDATION,
      MedusaLogInfoUtil.FOUNDATION_AC,MedusaLogInfoUtil.CLASSIFICATION,MedusaLogInfoUtil.LIVE_AC)
    val countBy = "userId"
    val countByIndex = MedusaLogInfoUtil.USERID_HOMEACCESS
    //    val restrictColumn = "accessArea"
    val restrictColumnIndex = MedusaLogInfoUtil.ACCESSAREA_HOMEACCESS
    val event = "click"
    val eventColumnIndex = MedusaLogInfoUtil.EVENT_HOMEACCESS
    val statisticType = ""
    val dateTime = MedusaLogInfoUtil.DATETIME_HOMEACCESS
    val insertTable = "medusa_launch_area_by_hour_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,hour,launch_area_click_pv,launch_area_click_uv) " +
      s"values (?,?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "accessArea"
    val eventColumnName = "event"
    val dateTimeColumnName = "datetime"
    StatisticsModel.pvuvByHourStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,dateTimeColumnName,eventColumnName)
  }

}
