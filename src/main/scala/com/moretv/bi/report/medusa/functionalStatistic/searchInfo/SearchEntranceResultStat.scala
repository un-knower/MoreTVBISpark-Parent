package com.moretv.bi.report.medusa.functionalStatistic.searchInfo

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 9/18/16.
  */

object SearchEntranceResultStat extends BaseClass {

  private val tableName = "search_entrance_result_dist"

  private val fields = "day,entrance,contentType,pv,uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]) {

    ModuleClass.executor(this, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val withNull = udf((s: String) => {
          s match {
            case _ => if (s == null) {
              "null"
            } else {
              s
            }
          }
        })

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        val sq = sqlContext
        import sq.implicits._

        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          val df = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.CLICKSEARCHRESULT, loadDate)
            .filter($"date" === sqlDate)
            .select(withNull($"entrance").as("entrance"), withNull($"contentType").as("contentType"), $"userId")
            .groupBy($"entrance", $"contentType")
            .agg(count($"userId").as("pv"), countDistinct($"userId").as("uv"))

          if (p.deleteOld) {
            util.delete(s"delete from $tableName where day = ?", sqlDate)
          }

          df.collect.foreach(w => {
            util.insert(insertSql, sqlDate, w.getString(0), w.getString(1), w.getLong(2), w.getLong(3))
          })

        })
      }
      case None => {
        throw new Exception("SearchEntranceResultStatistic fails")
      }

    }
  }
}
