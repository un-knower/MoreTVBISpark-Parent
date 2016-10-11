package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}

/**
 * Created by Administrator on 2016/4/15.
 */
object LauncherSearchAndSetStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(LauncherSearchAndSetStatistic,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK
    val statisticType = MedusaLogInfoUtil.SEARCHSET_AC
    val statisticByColumnName = "accessArea"
    val restrict = Array(MedusaLogInfoUtil.SEARCH_AL,MedusaLogInfoUtil.SET_AL)
    val countBy = "userId"
    val event = "click"
    val insertTable = "medusa_launch_searchandset_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launch_searchandset_click_pv," +
      s"launch_searchandset_click_uv) values (?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "accessLocation"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName,statisticByColumnName)
  }
}
