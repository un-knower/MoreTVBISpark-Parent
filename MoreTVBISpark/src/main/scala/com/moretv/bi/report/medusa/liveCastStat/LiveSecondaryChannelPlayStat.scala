package com.moretv.bi.report.medusa.liveCastStat

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.functions._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.LogType
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.types.DataType

import scala.collection.immutable
import scala.reflect.api.TypeTags

/**
  * Created by witnes on 2/14/17.
  */
object LiveSecondaryChannelPlayStat extends BaseClass {

  private val liveSecondaryChannelPlayDayTable = "live_channel_secondary_day_play_stat"

  private val fieldsForDayTable = "day, channelSid, liveName, uv, duration"

  private val insertSqlForDayTable =
    s"insert into $liveSecondaryChannelPlayDayTable($fieldsForDayTable) values(?,?,?,?,?)"

  private val deleteSqlForDayTable =
    s"delete from $liveSecondaryChannelPlayDayTable where day = ?"

  private val liveSecondaryChannelPlay10MinunteTable = "live_channel_secondary_day_play_stat"

  private val fieldsFor10MinunteTable = "day, channelSid, liveName, uv, duration"

  private val insertSqlFor10MinuteTable =
    s"insert into $liveSecondaryChannelPlay10MinunteTable($fieldsForDayTable) values(?,?,?,?,?)"

  private val deleteSqlFor10MinuteTable = s"delete from $liveSecondaryChannelPlay10MinunteTable where day = ?"


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val q = sqlContext
        import q.implicits._

        val cal = Calendar.getInstance

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val periodFillingWithStartEndUdf = udf[Seq[(Int, Int)], Int, Int, Int, Int](periodFillingWithStartEnd)

        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          val playDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogType.LIVE, loadDate)
            .filter($"liveType" === "live" && $"date" === sqlDate)
            .select($"channelSid", $"liveName", $"event", $"duration", $"liveMenuCode", $"userId", $"sourceType", $"datetime")

          if (p.deleteOld) {
            util.delete(deleteSqlForDayTable, sqlDate)
          }

          playDf
            .filter($"event" === "startplay")
            .groupBy($"channelSid", $"liveName", $"sourceType", $"liveMenuCode")
            .agg(countDistinct($"userId").as("uv"))
            .as("u").join(
            playDf.filter($"event" === "switchchannel" && $"duration".between(10, 36000))
              .groupBy($"channelSid", $"liveName", $"sourceType", $"liveMenuCode")
              .agg(sum($"duration").as("durationByDay")).as("d"),
            $"u.channelSid" === $"d.channelSid" && $"u.liveName" === $"d.liveName"
              && $"u.sourceType" === $"d.sourceType" && $"u.liveMenuCode" === $"d.liveMenuCode"
          )
            .select($"u.sourceType", $"u.liveMenuCode", $"u.channelSid", $"u.liveName", $"u.uv", $"d.durationByDay")
            .show(100, false)

          //            .collect
          //
          //            .foreach(w => {
          //              util.insert(
          //                insertSqlForDayTable, sqlDate, w.getString(0), w.getString(1), w.getLong(2), w.getLong(3)
          //              )
          //            })

          playDf
            .filter($"event" === "startplay")
            .groupBy(hour($"datetime").as("hour"), minute($"datetime").as("minute"),
              $"channelSid", $"liveName", $"sourceType", $"liveMenuCode")
            .agg(count($"userId").as("vv"))
            .show(100, false)

          playDf.filter($"event" === "switchchannel" && $"duration".between(10, 36000))
            .select(
              $"channelSid",
              $"liveMenuCode",
              $"liveName",
              $"userId",
              periodFillingWithStartEndUdf(
                hour((unix_timestamp($"datetime") - $"duration").cast("timestamp")),
                hour((unix_timestamp($"datetime").cast("timestamp"))),
                floor(minute((unix_timestamp($"datetime") - $"duration").cast("timestamp")) / 10) * 10,
                floor(minute(unix_timestamp($"datetime").cast("timestamp")) / 10) * 10
              ).as("period")
            )
            .withColumn("period", explode($"period"))
            .groupBy($"liveMenuCode", $"channelSid", $"liveName", $"period._1", $"period._2")
            .agg(countDistinct($"userId").as("uv"))
            .show(100, false)


        })

      }
      case None => {

      }
    }
  }


  def periodFillingWithStartEnd(startHour: Int, endHour: Int,
                                startMinute: Int, endMinute: Int): Seq[(Int, Int)] = {

    ((startHour :: Nil).cross(startMinute to 60 by 10)).toList ++
    ((startHour + 1 to endHour - 1 by 1).cross(0 to 60 by 10)).toList ++
    ((endHour :: Nil).cross(0 to endMinute by 10)).toList

  }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for {x <- xs; y <- ys} yield (x, y)
  }


}
