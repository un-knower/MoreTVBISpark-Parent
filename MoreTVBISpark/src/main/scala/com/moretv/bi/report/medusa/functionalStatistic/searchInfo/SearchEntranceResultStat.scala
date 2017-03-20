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

  private val tableName = "search_entrance_tabview_click_dist"

  private val fields = Array("day", "search_entrance", "tab_contentType", "click_contentType", "enter_num", "tabview_num", "click_num")

  private val insertSql = s"insert into $tableName(${fields.mkString(",")}) values(?,?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]) {


    ModuleClass.executor(this, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        val sq = sqlContext
        import sq.implicits._

        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          if (p.deleteOld) {
            util.delete(s"delete from $tableName where day = ?", sqlDate)
          }


          val entranceDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.SEARCH_ENTRANCE, loadDate)
            .filter($"date" === sqlDate)
            .groupBy($"date", $"entrance")
            .agg(count($"userId").as("enter_num"))


          val tabviewDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.SEARCH_TABVIEW, loadDate)
            .filter($"date" === sqlDate)
            .groupBy($"date", $"entrance", $"tabName")
            .agg(count($"userId").as("tabview_num"))



          val clickDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.SEARCH_CLICKRESULT, loadDate)
            .filter($"date" === sqlDate)
            .groupBy($"date", $"entrance", $"tabName", $"contentType")
            .agg(count($"userId").as("click_num"))



          entranceDf.join(tabviewDf, "date" :: "entrance" :: Nil)
            .join(clickDf, "date" :: "tabName" :: "entrance" :: Nil)
            .withColumnRenamed("date", "day").withColumnRenamed("entrance", "search_entrance")
            .withColumnRenamed("tabName", "tab_contentType").withColumnRenamed("contentType", "click_contentType")
            .select(fields.map(col(_)): _*)
            .collect
            .foreach(w => {

              util.insert(insertSql,

                w.getString(0), w.getString(1), w.getString(2), w.getString(3), w.getLong(4), w.getLong(5), w.getLong(6)
              )

            })


        })
      }
      case None => {
        throw new Exception("SearchEntranceResultStatistic fails")
      }

    }
  }
}
