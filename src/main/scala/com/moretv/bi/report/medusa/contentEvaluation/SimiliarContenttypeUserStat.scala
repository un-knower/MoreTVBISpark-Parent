package com.moretv.bi.report.medusa.contentEvaluation

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.report.medusa.userDevelop.PeriodUserDevelopStat._
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.zookeeper.KeeperException.DataInconsistencyException
import org.joda.time.format.DateTimeFormat

/**
  * Created by witnes on 3/30/17.
  */
object SimiliarContenttypeUserStat extends BaseClass {

  val readDateFormat = DateTimeFormat.forPattern("yyyyMMdd")

  val tbl = "medusa_similar_play_stat"

  val fields = Array(
    "day", "content_type", "play_user_num", "play_num", "play_num_per_user", "duration_per_user", "duration_per_time"
  )

  val deleteSql = s"delete from $tbl where day =?"

  val insertSql = s"insert into $tbl (${fields.mkString(",")}) values(${List.fill(fields.length)("?").mkString(",")})"


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val sq = sqlContext
        import sq.implicits._
        import org.apache.spark.sql.functions._

        var readDate = readDateFormat.parseLocalDate(p.startDate)

        val exporter = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        (0 until p.numOfDays).foreach(d => {

          val ods = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY, readDate.toString("yyyyMMdd"))

          readDate = readDate.minusDays(1)

          val similiarOds = ods.filter($"pathSub".like("%similar%"))


          if (p.deleteOld) {
            exporter.delete(deleteSql, readDate.toString("yyyy-MM-dd"))
          }

          similiarOds.filter($"event" === "startplay")
            .groupBy($"contentType")
            .agg(countDistinct($"userId").as("playUserNum"), count($"userId").as("playNum"))

            .join(

              similiarOds.filter($"event" !== "startplay").filter($"duration".between(1, 10800))
                .groupBy($"contentType")
                .
                  agg(sum($"duration").as("durationSum")),

              "contentType" :: Nil
            )
            .select(
              $"contentType", $"playUserNum", $"playNum",
              ($"playNum" / $"playUserNum").cast("double"),
              ($"durationSum" / $"playUserNum").cast("double"),
              ($"durationSum" / $"playNum").cast("double")
            )
            .collect
            .foreach(w => {

              exporter.insert(insertSql, readDate.toString("yyyy-MM-dd"),
                w.getString(0), w.getLong(1), w.getLong(2), w.getDouble(3), w.getDouble(4), w.getDouble(5))

            })


        })
      }
      case None => {

      }
    }
  }
}
