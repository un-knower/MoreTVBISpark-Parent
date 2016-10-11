package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}

/**
 * Created by Administrator on 2016/4/15.
 */
object LauncherFoundationStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(LauncherFoundationStatistic,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK
    val statisticType = MedusaLogInfoUtil.FOUNDATION_AC
    val statisticByColumnName = "accessArea"
    val restrict = Array(MedusaLogInfoUtil.INTERESTLOCATION_FOUNDATION_AL,MedusaLogInfoUtil.TOPNEW_FOUNDATION_AL,
      MedusaLogInfoUtil.TOPHOT_FOUNDATION_AL,MedusaLogInfoUtil.TOPSTAR_FOUNDATION_AL,MedusaLogInfoUtil.TOPCOLLECT_FOUNDATION_AL)
    val countBy = "userId"
    val event = "click"
    val insertTable = "medusa_launch_foundation_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launch_foundation_click_pv,launch_foundation_click_uv) values (?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "accessLocation"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName,statisticByColumnName)
  }
}
