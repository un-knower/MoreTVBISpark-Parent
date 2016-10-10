package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}

/**
 * Created by Administrator on 2016/4/16.
 */
object LauncherViewDurationStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(LauncherViewDurationStatistic,args)
  }
  override def execute(args: Array[String]): Unit = {
    val logType = MedusaLogInfoUtil.LAUNCHVIEW
    val sumBy = "duration"
    val insertTable = "medusa_launch_view_duration"
    val sqlInsert = s"insert into $insertTable(day,launcher_view_duration) values (?,?)"
    StatisticsModel.sumStatisticModel(args,sqlContext,logType,sumBy,insertTable,sqlInsert)
  }
}
