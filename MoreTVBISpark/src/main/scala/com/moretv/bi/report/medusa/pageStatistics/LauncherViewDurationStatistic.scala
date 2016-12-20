package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

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
