package com.moretv.bi.report.medusa.dataAnalytics

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.LogTypes
import com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.MVOminibusStat._
import com.moretv.bi.util.{ParamsParseUtil, TimeAggregator}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.joda.time.format.DateTimeFormat

/**
  * Created by witnes on 4/12/17.
  */
object FreqSniff extends BaseClass {

  val readDateFormat = DateTimeFormat.forPattern("yyyyMMdd")

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {


    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        val q = sqlContext
        import q.implicits._
        import org.apache.spark.sql.functions._

        var loadDate = readDateFormat.parseLocalDate(p.startDate)

        val timeAgg = new TimeAggregator

        (0 until p.numOfDays).foreach(d => {

          val ods = DataIO.getDataFrameOps
            .getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW, loadDate.toString("yyyyMMdd"))

          val df = ods.groupBy($"userId", $"videoSid")
            .agg(
              timeAgg(unix_timestamp($"datetime").cast("long")).as("timeDiff"),
              count($"datetime")
            )
          df.printSchema()
          df.show(10, false)

          //
          //          val aggOds =
          //
          //            ods.groupBy($"userId", $"videoSid")
          //              .agg(
          //                explode(
          //                  timeAgg(unix_timestamp($"datetime"))
          //                ).as("timeDiff")
          //              )
          //
          //
          //          aggOds
          //            .groupBy($"userId", $"timeDiff")
          //            .agg(count($"videoSid").as("user_play_num"))
          //            .orderBy($"timeDiff".asc, $"user_play_num".desc)
          //            .show(200, false)
          //
          //
          //          aggOds
          //            .groupBy($"videoSid", $"timeDiff")
          //            .agg(count($"userId").as("sid_play_num"))
          //            .orderBy($"timeDiff".asc, $"sid_play_num".desc)
          //            .show(200, false)

        })


      }
      case None => {

      }

    }
  }
}
