package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.pageStatistics.LauncherViewStatistic._
import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext

/**
 * Created by Administrator on 2016/4/25.
 */
object EnterApplicationStatistic extends BaseClass{
  def main (args: Array[String]) {
    ModuleClass.executor(EnterApplicationStatistic,args)
  }
  override def execute(args: Array[String]): Unit = {
    val logType = MedusaLogInfoUtil.ENTER
    val countBy = "userId"
    val insertTable = "medusa_enter_application_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,enter_pv,enter_uv) values (?,?,?)"
    StatisticsModel.pvuvStatisticModel(args,sqlContext,logType,countBy,insertTable,sqlInsert)
  }
}
