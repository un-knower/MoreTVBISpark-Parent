package com.moretv.bi.report.medusa.pageStatistics

import com.moretv.bi.report.medusa.pageStatistics.LauncherViewStatistic._
import com.moretv.bi.report.medusa.util.{MedusaLogInfoUtil, StatisticsModel}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext

/**
 * Created by Administrator on 2016/4/14.
 */
object EnterApplicationByProductStatistic extends BaseClass{

  def main (args: Array[String]) {
    ModuleClass.executor(EnterApplicationByProductStatistic,args)
  }
 override def execute(args: Array[String]) {
    val logType = MedusaLogInfoUtil.ENTER
    val restrict = Array("LetvNewC1S","we20s","M321","MagicBox_M13","MiBOX3")
    val countBy = "userId"
    val event = "enter"
    val statisticType = ""
    val insertTable = "medusa_enter_by_product_pv_uv"
    val sqlInsert = s"insert into $insertTable(day,product_model_index,product_model,launch_area_click_pv," +
      s"launch_area_click_uv) values" +
      s" (?,?,?,?,?)"
    val countByColumnName = "userId"
    val restrictByColumnName = "productModel"
    val eventColumnName = "event"
    StatisticsModel.pvuvRestrictStatisticModel(args,sqlContext,logType,event,statisticType,
      countBy,restrict,insertTable,sqlInsert,countByColumnName,restrictByColumnName,eventColumnName)
  }
}
