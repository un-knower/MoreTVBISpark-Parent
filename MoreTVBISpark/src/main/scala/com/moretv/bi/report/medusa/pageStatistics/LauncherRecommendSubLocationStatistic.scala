package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{MedusaLogInfoUtil, StatisticsModel}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by Administrator on 2016/4/15.
 */
object LauncherRecommendSubLocationStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK
    val statisticType = "recommendation"
    val restrict = Array("0","1","2","3","4","5","6","7","8","9","10","11","12","13","14")
    val countBy = "userId"
    val event = "click"
    val insertTable = "medusa_launch_recommend_sublocation_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,launch_recommendation_click_pv," +
      s"launch_recommendation_click_uv) values (?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "locationIndex"
    val eventColumnName = "event"
    val statisticByColumnName = "accessArea"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName,statisticByColumnName)
  }
}
