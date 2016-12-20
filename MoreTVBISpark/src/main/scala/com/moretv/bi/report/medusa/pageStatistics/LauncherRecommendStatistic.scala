package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by Administrator on 2016/4/15.
 */
object LauncherRecommendStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(LauncherRecommendStatistic,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK
    val statisticType = ""
    val restrict = Array(MedusaLogInfoUtil.RECOMMENDATION_AC)
    val countBy = "userId"
    val event = "click"
    val insertTable = "medusa_launch_recommendation_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launch_recommendation_click_pv," +
      s"launch_recommendation_click_uv) values (?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "accessArea"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName)
  }
}
