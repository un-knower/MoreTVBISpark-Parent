package com.moretv.bi.report.medusa.contentEvaluation

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.report.medusa.liveCastStat.PeriodLivePlayStat._
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.joda.time.format.DateTimeFormat

/**
  * Created by witnes on 4/10/17.
  */
object GuessYouLikeStat extends BaseClass {

  val readFormat = DateTimeFormat.forPattern("yyyyMMdd")

  val sqlFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

  val tbl = "medusa_guessyoulike_type_paly_stat"

  val fields = Array("day", "type", "play_num", "play_user_num", "duration")

  val insertSql = s"insert into $tbl (${fields.mkString(",")}) values(${List.fill(fields.length)("?").mkString(",")})"

  val deleteSql = s"delete from $tbl where day = ?"


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {


    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sq = sqlContext
        import sq.implicits._
        import org.apache.spark.sql.functions._
        val exporter = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        var loadDate = readFormat.parseLocalDate(p.startDate)

        (0 until p.numOfDays).foreach(d => {

          if (p.deleteOld) {
            exporter.delete(deleteSql, loadDate.minusDays(1).toString("yyyy-MM-dd"))
          }


          val ods = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY, loadDate.toString("yyyyMMdd"))

          loadDate = loadDate.minusDays(1)

          val df = ods.filter($"pathMain".like("%movie*猜你喜欢%"))
            .withColumn("type", lit("电影猜你喜欢"))

            .unionAll(
              ods.filter($"pathMain".like("%tv*猜你喜欢%"))
                .withColumn("type", lit("电视剧猜你喜欢"))
            )
            .unionAll(
              ods.filter($"pathSub".like("%guessyoulike%mv%"))
                .withColumn("type", lit("音乐退出推荐"))
            )



          df.filter($"event" === "startplay")
            .groupBy($"type")
            .agg(count($"userId"), countDistinct($"userId"))

            .join(
              df.filter($"event" !== "startplay").filter($"duration".between(1, 10800))
                .groupBy($"type")
                .agg(sum($"duration")),

              "type" :: Nil
            )
            .collect().foreach(r => {
            exporter.insert(insertSql, loadDate.toString("yyyy-MM-dd"),
              r.get(0), r.get(1), r.get(2), r.get(3))
          })


        })


      }

      case None => {

      }
    }
  }
}
