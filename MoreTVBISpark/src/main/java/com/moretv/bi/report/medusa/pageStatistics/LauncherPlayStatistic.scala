package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{MedusaLogInfoUtil, StatisticsModel}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}


/**
 * Created by Administrator on 2016/4/14.
 */
object LauncherPlayStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(LauncherPlayStatistic,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.PLAY
    val restrict = Array(MedusaLogInfoUtil.RECOMMENDATION)
    val countBy = "userId"
    val statisticType = ""
    val event = "startplay"
    val insertTable = "medusa_launch_play_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launcher_play_pv,launcher_play_uv) " +
      s"values (?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "pathMain"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName)
  }
}
