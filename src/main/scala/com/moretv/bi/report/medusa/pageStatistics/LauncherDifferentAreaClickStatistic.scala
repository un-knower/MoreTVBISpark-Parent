package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.pageStatistics.LauncherViewStatistic._
import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext

/**
 * Created by Administrator on 2016/4/14.
 */
object LauncherDifferentAreaClickStatistic extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK

    val restrict = Array(MedusaLogInfoUtil.SEARCHSET_AC,MedusaLogInfoUtil.MYTV_AC,MedusaLogInfoUtil.RECOMMENDATION,
      MedusaLogInfoUtil.FOUNDATION_AC, MedusaLogInfoUtil.CLASSIFICATION, MedusaLogInfoUtil.LIVE_AC,
      MedusaLogInfoUtil.HOT_SUBJECT_AC, MedusaLogInfoUtil.TASTE_AC)
    val countBy = "userId"
    val event = "click"
    val statisticType = ""
    val insertTable = "medusa_launch_area_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launch_area_click_pv,launch_area_click_uv) values (?," +
      s"?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "accessArea"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName)
  }
}
