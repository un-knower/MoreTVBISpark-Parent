package com.moretv.bi.report.medusa.util

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.DataFrame
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, LocalDate}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 3/29/17.
  */
abstract class PeriodJob extends BaseClass {

  // general format
  val readFormat = DateTimeFormat.forPattern("yyyyMMdd")

  val sqlFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

  // input

  var pathPatterns: Seq[String] = _

  var logTypes: Seq[String] = _


  // output

  var tbl: String = _

  var fields: Array[String] = _

  var isDeleteOld: Boolean = _

  var deleteSql: String = _

  var insertSql: String = _


  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        var dateCondition = readFormat.parseLocalDate(p.startDate)

        deleteSql =
          s"delete from $tbl where execute_day = ? and period_type = ? and period_value = ?"

        insertSql =
          s"insert $tbl (${fields.mkString(",")}) values(${List.fill(fields.length)("?").mkString(",")})"

        isDeleteOld = p.deleteOld

        (0 until p.numOfDays).foreach(d => {
          dateMatchPeriod(dateCondition)

          dateCondition = dateCondition.minusDays(1)
        })

      }
      case None => {
        println("--")
      }
    }
  }

  def periodAggHandle(periodType: String, periodValue: String, givenDate: LocalDate): Unit


  def dateMatchPeriod(dateCondition: LocalDate): Unit = {

    if (dateCondition.getDayOfMonth == 1) {

      periodAggHandle(PeriodType.MONTH, dateCondition.getMonthOfYear.toString, dateCondition)

      if ((4 :: 7 :: 10 :: Nil).contains(dateCondition.getMonthOfYear)) {

        periodAggHandle(PeriodType.QUARTER,
          (dateCondition.getMonthOfYear / 3).toString, dateCondition)
      }

      else if (dateCondition.getMonthOfYear == 1) {

        periodAggHandle(PeriodType.QUARTER,
          (dateCondition.getMonthOfYear / 3).toString, dateCondition)

        periodAggHandle(PeriodType.YEAR, dateCondition.getYear.toString, dateCondition)

      }

    }

    if (dateCondition.getDayOfWeek == 1) {

      periodAggHandle(PeriodType.WEEK, dateCondition.getWeekOfWeekyear.toString, dateCondition)

    }

    periodAggHandle(PeriodType.DAY, dateCondition.minusDays(1).toString("yyyy-MM-dd"), dateCondition)

  }

  /**
    * partitioned by period in path
    *
    * @param periodType
    * @param numOfPeriods
    * @param endDate
    * @return
    */
  def getPeriodPartition(periodType: String,
                         numOfPeriods: Int, endDate: LocalDate,
                         logType: String, pathPattern: String): DataFrame = {


    val startDate = periodType match {
      case PeriodType.DAY => {
        endDate.minusDays(numOfPeriods)
      }
      case PeriodType.WEEK => {
        endDate.minusWeeks(numOfPeriods)
      }
      case PeriodType.MONTH => {
        endDate.minusMonths(numOfPeriods)
      }
      case PeriodType.QUARTER => {
        endDate.minusMonths(3 * numOfPeriods)
      }
      case PeriodType.YEAR => {
        endDate.minusYears(numOfPeriods)
      }
      case _ => {
        endDate
      }
    }

    val diffDays = Days.daysBetween(startDate, endDate).getDays

    val dates = (1 to diffDays).map(i => {
      startDate.plusDays(i).toString("yyyyMMdd")
    }).toArray


    DataIO.getDataFrameOps.getDF(sc, Map[String, String](), pathPattern, logType, dates)

  }


  /**
    * partitioned by period in filtering field (openTime)
    */
  def getPeriodSnapshot(periodType: String, numOfPeriods: Int, endDateN: LocalDate,
                        logType: String, pathPattern: String): DataFrame = {

    val sq = sqlContext
    import sq.implicits._

    val endDate = endDateN.minusDays(1)

    val snapshotTbl = DataIO.getDataFrameOps.getDF(
      sc, Map[String, String](), pathPattern, logType, endDate.toString("yyyyMMdd"))

    val startDate = periodType match {

      case PeriodType.DAY => {
        endDate.minusDays(numOfPeriods)
      }

      case PeriodType.WEEK => {
        endDate.minusWeeks(numOfPeriods)
      }

      case PeriodType.MONTH => {
        endDate.minusMonths(numOfPeriods)
      }

      case PeriodType.QUARTER => {
        endDate.minusMonths(numOfPeriods * 3)
      }

      case PeriodType.YEAR => {
        endDate.minusYears(numOfPeriods)
      }

      case _ => {
        endDate.minusDays(numOfPeriods)
      }
    }


    snapshotTbl.filter(
      to_date($"openTime").between(startDate.plusDays(1).toString("yyyy-MM-dd"), endDate.toString("yyyy-MM-dd"))
    )

  }


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }


  object PeriodType {

    val DAY = "day"

    val WEEK = "week"

    val MONTH = "month"

    val QUARTER = "quarter"

    val YEAR = "year"
  }


}

