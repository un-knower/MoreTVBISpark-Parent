package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by Administrator on 2016/4/15.
 */
object LauncherMyTVStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(LauncherMyTVStatistic,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK
    val statisticType = MedusaLogInfoUtil.MYTV_AC
    val statisticByColumnName = "accessArea"
    val restrict = Array(MedusaLogInfoUtil.HISTORY_MYTV_AL,MedusaLogInfoUtil.COLLECT_MYTV_AL,MedusaLogInfoUtil
      .ACCOUNT_MYTV_AL,MedusaLogInfoUtil.SITEMOVIE_MYTV_AL,MedusaLogInfoUtil.SITETV_MYTV_AL,MedusaLogInfoUtil
      .SITEZONGYI_MYTV_AL,MedusaLogInfoUtil.SITESPORT_MYTV_AL,MedusaLogInfoUtil.SITELIVE_MYTV_AL,MedusaLogInfoUtil
      .SITECOMIC_MYTV_AL,MedusaLogInfoUtil.SITEJILU_MYTV_AL,MedusaLogInfoUtil.SITEHOT_MYTV_AL,MedusaLogInfoUtil
      .SITEMV_MYTV_AL,MedusaLogInfoUtil.SITEKIDS_MYTV_AL,MedusaLogInfoUtil.SITEXIQU_MYTV_AL,MedusaLogInfoUtil.SITEAPPLICATION_MYTV_AL)
    val countBy = "userId"
    val event = "click"
    val insertTable = "medusa_launch_my_tv_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launch_my_tv_click_pv,launch_my_tv_click_uv) values " +
      s"(?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "accessLocation"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName,statisticByColumnName)
  }
}
