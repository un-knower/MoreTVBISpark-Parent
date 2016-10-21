package com.moretv.bi.temp

import com.moretv.bi.report.medusa.channeAndPrograma.mv.MVRecommendPlay._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 10/20/16.
  */
object LiveRatioCount extends BaseClass {

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "8g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "60")
    ModuleClass.executor(LiveRatioCount, args)
  }

  override def execute(args: Array[String]): Unit = {

    val loadLivePath = "/log/medusa/parquet/2016{09*}/live"

    val loadPlayPath = "/log/medusa/parquet/2016{09*}/play"


    val dfLive = sqlContext.read.parquet(loadLivePath)
      .select("userId", "duration")
      .filter("duration between 0 and 10800")

    dfLive.registerTempTable("log_data_live")

    val dfPlay = sqlContext.read.parquet(loadPlayPath)
      .select("userId", "duration")
      .filter("duration between 0 and 10800")

    dfPlay.registerTempTable("log_data_play")


    val userAll = 13213586

    //    val userLiveNotPlay = dfLive.select("userId").distinct
    //      .except(dfPlay.select("userId").distinct).count
    //
    //    val ratioLiveNotPlay = userLiveNotPlay.toFloat / userAll
    //

    val ratioLiveMorePlay = sqlContext.sql(
      s"""
         |select live.userId
         |from log_data_live as live
         |inner join log_data_play as play on live.userId = play.userId
         |where live.duration > play.duration
         |group by live.userId
       """.stripMargin
    ).count.toFloat / userAll

    println(ratioLiveMorePlay)

    val ratioliveMorePlay30 = sqlContext.sql(
      s"""
         |select live.userId
         |from log_data_live as live
         |inner join log_data_play as play on live.userId = play.userId
         |where live.duration - play.duration > 1800
         |group by live.userId
       """.stripMargin
    ).count.toFloat / userAll

    println(ratioliveMorePlay30)

    val ratioliveMorePlay60 = sqlContext.sql(
      s"""
         |select live.userId
         |from log_data_live as live
         |inner join log_data_play as play on live.userId = play.userId
         |where live.duration - play.duration > 3600
         |group by live.userId
       """.stripMargin
    ).count.toFloat / userAll

    println(ratioliveMorePlay60)

    //    val df = sqlContext.sql(
    //      s"""
    //         |select c1/$userAll  as r1, c1
    //         |,c2/$userAll as r2, c2
    //         |,c3/$userAll as r3, c3
    //         |from (
    //         |select
    //         |count(case when live.duration > play.duration then 1 else 0 end)  as c1
    //         |,count(case when live.duration - play.duration > 1800  then 1 else 0 end) as c2
    //         |,count(case when live.duration - play.duration > 3600 then 1 else 0 end) as c3
    //         |from log_data_live as live
    //         |inner join log_data_play as play on live.userId = play.userId
    //         |) a
    //      """.stripMargin)
    //
    //
    //    df.show(10, false)


  }

}
