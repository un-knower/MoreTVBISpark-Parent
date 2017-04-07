package com.moretv.bi.report.medusa.liveCastStat


import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.report.medusa.util.PeriodJob
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.DataFrame
import org.joda.time.{Days, LocalDate}

import scala.collection.mutable.ListBuffer


/**
  * Created by witnes on 3/28/17.
  */

/**
  * Period include month , week, day
  *
  */
object PeriodLivePlayStat extends PeriodJob {

  pathPatterns = MEDUSA :: Nil

  logTypes = LogTypes.LIVE :: Nil

  tbl = "live_period_play_stat"

  fields = Array[String](
    "execute_day",
    "period_type",
    "period_value",
    "user_count",
    "duration_sum"
  )


  override def periodAggHandle(periodType: String, periodValue: String, givenDate: LocalDate): Unit = {

    val metrics = ListBuffer[Long]()
    val exporter = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

    (logTypes zip pathPatterns).foreach {
      case (logType, pathPattern) => {
        logType match {
          case LogTypes.LIVE => {

            val res1 = aggregateLiveUserStat(
              getPeriodPartition(periodType, 1, givenDate, logType, pathPattern)
            )

            metrics.append(res1._1, res1._2)
          }
        }
      }
    }

    val li = metrics.toList
    if (isDeleteOld) {
      exporter.delete(deleteSql, givenDate.toString("yyyy-MM-dd"), periodType, periodValue)
    }

    exporter.insert(insertSql,
      givenDate.toString("yyyy-MM-dd"),
      periodType, periodValue, li(0), li(1)
    )

  }

  def aggregateLiveUserStat(ods: DataFrame): (Long, Long) = {

    val sq = sqlContext
    import sq.implicits._
    import org.apache.spark.sql.functions._

    val userCount = ods.filter($"event" === "startplay")
      .agg(countDistinct($"userId"))
      .first.getLong(0)

    val durationSum = ods.filter($"event" !== "startplay")
      .filter($"duration".between(1, 360000))
      .agg(sum($"duration"))
      .first.getLong(0)

    (userCount, durationSum)

  }


}
