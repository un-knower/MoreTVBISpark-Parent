package com.moretv.bi.report.medusa.liveCastStat

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.LogType
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 1/12/17.
  */

/**
  * Begin with Medusa 3.1.2
  *
  * logType : Play
  * Filter : liveType = live
  * * GroupBy : SourceType
  * Stat : uv , vv, duration
  */
object LiveSourceTypePlayStat extends BaseClass {

  private val tableName1 = "live_day_sourcetype_stat"

  private val tableName2 = "live_hour_sourcetype_stat"

  private val fields1 = "day,sourceType,vv,uv,duration"

  private val fields2 = "day,hour,sourceType,uv"

  private val insertSql1 = s"insert into $tableName1($fields1)values(?,?,?,?,?)"

  private val insertSql2 = s"insert into $tableName2($fields2)values(?,?,?,?)"

  private val deleteSql1 = s"delete from $tableName1 where day = ? "

  private val deleteSql2 = s"delete from $tableName2 where day = ? "

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

        val typeFormat = udf((s: String) => {
          s match {
            case "telecast" => "电台直播"
            case "webcast" => "网络直播"
            case _ => "其它直播"
          }
        })

        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)


          val baseDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogType.LIVE, loadDate)
            .filter($"date" === sqlDate && $"liveType" === "live")
            .withColumn("sourceType", typeFormat($"sourceType"))


          if (p.deleteOld) {
            util.delete(deleteSql1, sqlDate)
            util.delete(deleteSql2, sqlDate)
          }

          // day

          baseDf.filter($"event" === "startplay")
            .groupBy($"date", $"sourceType")
            .agg(count($"userId").as("vv"), count($"userId").as("uv"))
            .as("t1")
            .join(
              baseDf.filter($"event" === "switchchannel" && $"duration".between(10, 36000))
                .groupBy($"date", $"sourceType")
                .agg(sum($"duration").as("duration")).as("t2"),
              $"t1.date" === $"t2.date" && $"t1.sourceType" === $"t2.sourceType"
            )
            .select($"t1.date", $"t1.sourceType", $"t1.vv", $"t1.uv", $"t2.duration")
            .collect.foreach(e => {
            util.insert(insertSql1, e.getString(0), e.getString(1), e.getLong(2), e.getLong(3), e.getLong(4))
          })

          // hour

          baseDf.filter($"event" === "startplay")
            .groupBy($"date", hour($"datetime").as("hour"), $"sourceType")
            .agg(countDistinct($"userId"))
            .collect.foreach(e => {
            util.insert(insertSql2, e.getString(0), e.getInt(1), e.getString(2), e.getLong(3))
          })

        })

      }
      case None => {

      }
    }
  }
}
