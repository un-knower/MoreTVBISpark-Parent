package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.pageStatistics.LauncherViewStatistic._
import com.moretv.bi.report.medusa.util.{StatisticsModel, MedusaLogInfoUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext

/**
 * Created by Administrator on 2016/4/15.
 */
object LauncherClassificationStatistic extends BaseClass{
  def main (args: Array[String]) {
    ModuleClass.executor(LauncherClassificationStatistic,args)
  }
  override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.LAUNCHCLICK
    val statisticType = MedusaLogInfoUtil.CLASSIFICATION_AC
    val statisticByColumnName = "accessArea"
    val restrict = Array(MedusaLogInfoUtil.MOVIE_CLASSIFICATION_AL,MedusaLogInfoUtil.TV_CLASSIFICATION_AL,MedusaLogInfoUtil
      .ZONGYI_CLASSIFICATION_AL,MedusaLogInfoUtil.SPORT_CLASSIFICATION_AL,MedusaLogInfoUtil
      .COMIC_CLASSIFICATION_AL,MedusaLogInfoUtil.JILU_CLASSIFICATION_AL,MedusaLogInfoUtil.HOT_CLASSIFICATION_AL,MedusaLogInfoUtil
      .MV_CLASSIFICATION_AL,MedusaLogInfoUtil.KIDS_CLASSIFICATION_AL,MedusaLogInfoUtil.XIQU_CLASSIFICATION_AL,MedusaLogInfoUtil.APPLICATION_CLASSIFICATION_AL)
    val countBy = "userId"
    val event = "click"
    val insertTable = "medusa_launch_classification_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,area_id,area_name,launch_classification_click_pv," +
      s"launch_classification_click_uv) values (?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "accessLocation"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName,statisticByColumnName)
  }
}
