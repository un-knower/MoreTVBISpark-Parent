package com.moretv.bi.temp

import com.moretv.bi.report.medusa.channeAndPrograma.mv.MVRecommendPlay._
import com.moretv.bi.report.medusa.newsRoomKPI.HotChannelPlayInfo._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 10/20/16.
  */
object LiveRatioCount extends BaseClass {

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "8g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(LiveRatioCount, args)
  }

  override def execute(args: Array[String]): Unit = {

    val loadLivePath = "/log/medusaAndMoretvMerger/2016{09*,1001}/live"

    val loadPlayPath = "/log/medusaAndMoretvMerger/2016{09*,1001}/playview"


    sqlContext.read.parquet(loadLivePath)
      .filter("duration between 0 and 10800")
      .filter("event ='switchchannel'")
      .registerTempTable("log_live")

    val dfLive = sqlContext.sql("select userId, sum(duration) as live_duration from log_live where " +
      "date between '2016-09-01' and '2016-09-30' group by userId ")

    sqlContext.read.parquet(loadPlayPath)
      .filter("event in ('userexit','selfend')")
      .filter("duration between 0 and 10800")
      .registerTempTable("log_play")

    val dfPlay = sqlContext.sql("select userId, sum(duration) as play_duration from log_play " +
      "where duration between 0 and 10800 group by userId")

    //    val userLiveNotPlay = dfLive.select("userId").distinct
    //      .except(dfPlay.select("userId").distinct).count
    //
    //    val ratioLiveNotPlay = userLiveNotPlay.toFloat / userAll

//    val LiveNotPlay = dfLive.select("userId").distinct
//          .except(dfPlay.select("userId").distinct).count
//
//    val LiveMorePlay_ = dfLive
//      .join(dfPlay,"userId")
//      .filter("live_duration > play_duration")
//      .select("userId")
//      .distinct
//      .count
//
//    println(LiveMorePlay_)


    val LiveMorePlay_30 = dfLive
      .join(dfPlay,"userId")
      .filter("(live_duration - play_duration) > 18000")
      .select("userId")
      .distinct
      .count

    println(LiveMorePlay_30)
//
//    val LiveMorePlay_60 = dfLive
//      .join(dfPlay,"userId")
//      .filter("(live_duration - play_duration) > 3600")
//      .distinct
//      .count
//
//    println(LiveMorePlay_60)
//
//    val LiveMorePlay_0_30 = dfLive
//      .filter("live_duration between 0 and 1800")
//      .select("userId")
//      .distinct
//      .count
//
//    println(LiveMorePlay_0_30)
//
//
//    val LiveMorePlay_30_60 = dfLive
//      .filter("live_duration between 1801 and 3600")
//      .select("userId")
//      .distinct
//      .count
//
//    println(LiveMorePlay_30_60)
//
//    val LiveMorePlay_60_90 = dfLive
//      .filter("live_duration between 3601 and 5400")
//      .distinct
//      .count
//    println(LiveMorePlay_60_90)
//
//    val LiveMorePlay_90_120 = dfLive
//      .filter("live_duration between 5401 and 7200")
//      .distinct
//      .count
//    println(LiveMorePlay_90_120)
//
//    val LiveMorePlay_120_ = dfLive
//      .filter("live_duration > 7200")
//      .distinct
//      .count
//    println(LiveMorePlay_120_)





    //    val ratioLiveMorePlay = sqlContext.sql(
    //      s"""
    //         |select distinct live.userId
    //         |from log_data_live as live
    //         |inner join log_data_play as play on live.userId = play.userId
    //         |where live.duration > play.duration
    //       """.stripMargin
    //    ).count.toFloat / userAll
    //
    //    println(ratioLiveMorePlay)

    //    val ratioliveMorePlay30 = sqlContext.sql(
    //      s"""
    //         |select distinct live.userId
    //         |from log_data_live as live
    //         |inner join log_data_play as play on live.userId = play.userId
    //         |where live.duration - play.duration > 1800
    //       """.stripMargin
    //    ).count.toFloat / userAll
    //
    //    println(ratioliveMorePlay30)
    //
    //    val ratioliveMorePlay60 = sqlContext.sql(
    //      s"""
    //         |select distinct live.userId
    //         |from log_data_live as live
    //         |inner join log_data_play as play on live.userId = play.userId
    //         |where live.duration - play.duration > 3600
    //       """.stripMargin
    //    ).count.toFloat / userAll
    //
    //    println(ratioliveMorePlay60)

    //    val df = sqlContext.sql(
    //      s"""
    //         |select c1/$userAll  as r1, c1
    //         |,c2/$userAll as r2, c2
    //         |,c3/$userAll as r3, c3
    //         |from (
    //         |select
    //         |count(distinct case when live.duration > play.duration then 1 else 0 end)  as c1
    //         |,count(distinct case when live.duration - play.duration > 1800  then 1 else 0 end) as c2
    //         |,count(distinct case when live.duration - play.duration > 3600 then 1 else 0 end) as c3
    //         |from log_data_live as live
    //         |inner join log_data_play as play on live.userId = play.userId
    //         |) a
    //          """.stripMargin)


  }

}
