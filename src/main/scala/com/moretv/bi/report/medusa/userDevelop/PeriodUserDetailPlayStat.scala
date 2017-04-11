package com.moretv.bi.report.medusa.userDevelop

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.report.medusa.util.PeriodJob
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 3/29/17.
  */
object PeriodUserDetailPlayStat extends PeriodJob {

  pathPatterns = MERGER :: MERGER :: Nil

  logTypes = LogTypes.DETAIL :: LogTypes.PLAYVIEW :: Nil

  tbl = "period_user_detail_play_stat"

  fields = Array(
    "execute_day",
    "period_type",
    "period_value",
    "detail_user_count",
    "detail_count",
    "play_user_count",
    "play_count",
    "play_duration_sum"
  )


  override def periodAggHandle(periodType: String, periodValue: String, givenDate: LocalDate): Unit = {

    val metrics = ListBuffer[Long]()
    (logTypes zip pathPatterns).foreach {

      case (logType, pathPattern) => {

        logType match {
          case LogTypes.DETAIL => {
            val res1 = aggregateDetailUserStat(
              getPeriodPartition(periodType, 1, givenDate, logType, pathPattern)
            )
            metrics.append(res1._1, res1._2)

          }
          case LogTypes.PLAYVIEW => {
            val res2 = aggregatePlayUserStat(
              getPeriodPartition(periodType, 1, givenDate, logType, pathPattern)
            )
            metrics.append(res2._1, res2._2, res2._3)
          }
          case _ => {
            null
          }
        }

      }
    }


    val exporter = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

    val li = metrics.toList
    if (isDeleteOld) {
      exporter.delete(deleteSql, givenDate.toString("yyyy-MM-dd"), periodType, periodValue)
    }

    exporter.insert(insertSql,
      givenDate.toString("yyyy-MM-dd"),
      periodType, periodValue, li(0), li(1), li(2), li(3), li(4)
    )

  }


  def aggregatePlayUserStat(ods: DataFrame): (Long, Long, Long) = {

    val sq = sqlContext

    import sq.implicits._
    import org.apache.spark.sql.functions._

    val row = ods.filter($"event".isin("startplay", "playview"))

      .agg(countDistinct($"userId"), count($"userId"))

      .first()

    val playUserCount = row.getLong(0)

    val playCount = row.getLong(1)

    val durationSum = ods.filter($"event" !== "startplay").filter($"duration".between(1, 10800))

      .agg(sum($"duration"))

      .first()

      .getLong(0)

    (playUserCount, playCount, durationSum)

  }


  def aggregateDetailUserStat(ods: DataFrame): (Long, Long) = {

    val sq = sqlContext

    import sq.implicits._
    import org.apache.spark.sql.functions._

    val row = ods.agg(countDistinct($"userId"), count($"userId"))
      .first()

    val detailUserCount = row.getLong(0)

    val detailCount = row.getLong(1)

    (detailUserCount, detailCount)

  }


}
