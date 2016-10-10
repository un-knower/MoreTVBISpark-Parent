package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{MedusaLogInfoUtil, StatisticsModel}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}

/**
 * Created by Administrator on 2016/4/14.
 */
object LauncherDifferentAreaPlayStatistic extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(LauncherDifferentAreaPlayStatistic,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.PLAY

    val restrict = Array(MedusaLogInfoUtil.SEARCHSET_AC,MedusaLogInfoUtil.MYTV_AC,MedusaLogInfoUtil.RECOMMENDATION,
      MedusaLogInfoUtil.FOUNDATION_AC,MedusaLogInfoUtil.CLASSIFICATION,MedusaLogInfoUtil.LIVE_AC)
    val countBy = "userId"
    val event = "startplay"
    val statisticType = ""
    val insertTable = "medusa_launch_area_play_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launch_area_play_pv,launch_area_play_uv) values (?," +
      s"?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "accessArea"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName)
  }
}
