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
object LauncherClickStatistic extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]): Unit = {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK
    val countBy = "userId"
    val insertTable = "medusa_launch_click_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,launch_click_pv,launch_click_uv) values (?,?,?)"
    StatisticsModel.pvuvStatisticModel(args,sqlContext,logType,countBy,insertTable,sqlInsert)
  }
}
