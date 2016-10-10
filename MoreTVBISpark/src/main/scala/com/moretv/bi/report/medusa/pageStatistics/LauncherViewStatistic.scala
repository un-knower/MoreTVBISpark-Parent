package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}

/**
 * Created by Administrator on 2016/4/14.
 */
object LauncherViewStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(LauncherViewStatistic,args)
  }
  override def execute(args: Array[String]): Unit = {
    val logType = MedusaLogInfoUtil.LAUNCHVIEW
    val countBy = "userId"
    val insertTable = "medusa_launch_view_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,launcher_view_pv,launcher_view_uv) values (?,?,?)"
    StatisticsModel.pvuvStatisticModel(args,sqlContext,logType,countBy,insertTable,sqlInsert)
  }
}
