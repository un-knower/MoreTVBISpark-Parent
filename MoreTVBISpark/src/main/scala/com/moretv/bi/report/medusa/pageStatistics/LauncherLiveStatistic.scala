package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}

/**
 * Created by Administrator on 2016/4/15.
 */
object LauncherLiveStatistic extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(LauncherLiveStatistic,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK
    val statisticType = MedusaLogInfoUtil.LIVE_AC
    val restrict = Array(MedusaLogInfoUtil.ONE_LIVE_LA,MedusaLogInfoUtil.TWO_LIVA_LA,MedusaLogInfoUtil
      .THREE_LIVE_LA,MedusaLogInfoUtil.FOUR_LIVE_LA,MedusaLogInfoUtil
      .FIVE_LIVE_LA,MedusaLogInfoUtil.SIX_LIVE_LA,MedusaLogInfoUtil.SEVEN_LIVE_LA,MedusaLogInfoUtil
      .EIGHT_LIVE_LA,MedusaLogInfoUtil.NINE_LIVE_LA,MedusaLogInfoUtil.TEN_LIVE_LA,MedusaLogInfoUtil
      .ELEVEN_LIVE_LA,MedusaLogInfoUtil.TWELEVE_LIVE_LA,MedusaLogInfoUtil.THIRTEEN_LIVE_LA)
    val countBy = "userId"
    val event = "click"
    val insertTable = "medusa_launch_live_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launch_live_click_pv," +
      s"launch_live_click_uv) values (?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "locationIndex"
    val eventColumnName = "event"
    val statisticByColumnName = "accessArea"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName,statisticByColumnName)
  }
}
