package com.moretv.bi.report.medusa.functionalStatistic.searchInfo

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import SearchEntranceResultStat._
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 1/14/17.
  */
object SearchTabViewStat extends BaseClass {

  private val tableName = "search_entrance_tabview_dist"

  private val fields = "day,entrance,contentType,pv,uv"

  private val insertSql = s"insert into $tableName($fields)values(?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val sq = sqlContext
        import sq.implicits._

        val withNull = udf((s: String) => {
          s match {
            case _ => if (s == null) {
              "null"
            } else {
              s
            }
          }
        })

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          val df = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.SEARCH_TABVIEW, loadDate)
            .filter($"date" === sqlDate)
            .select(withNull($"entrance").as("entrance"), $"tabName", $"userId")
            .groupBy($"entrance")
            .agg(count($"userId").as("pv"), countDistinct($"userId").as("uv"))
            .collect.foreach(e => {
            util.insert(insertSql, sqlDate, e.getString(0), e.getLong(1), e.getLong(2))
          })

        })
      }
      case None => {

      }
    }

  }
}
