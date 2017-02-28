package com.moretv.bi.temp.annual

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by witnes on 1/9/17.
  */
object LiveUserStat extends BaseClass {

  def main(args: Array[String]) {

    ModuleClass.executor(this, args)

  }

  override def execute(args: Array[String]): Unit = {

    val q = sqlContext
    import q.implicits._


    val pathForLive3 = "/log/medusaAndMoretvMerger/2016*/live"

    val pathForLive2 = "/mbi/parquet/live/{201601*,201602*,2016030*,2016031*,20160320,20160321}"

    val pathForPlay3 = "/log/medusaAndMoretvMerger/2016*/playview"

    val pathForPlay2 = "/mbi/parquet/playview/{201601*,201602*,2016030*,2016031*,20160320,20160321}"


    val liveDf = sqlContext.read.parquet(pathForLive2 :: pathForLive3 :: Nil: _*)
      .filter($"date" between("2016-01-01", "2016-12-31"))
      .filter($"liveType" === "live")


    liveDf.select($"userId")
      .agg(count($"userId").as("vv"), countDistinct($"userId").as("uv"))
      .show(100, false)

    liveDf.filter($"duration" between(10, 36000)).agg(sum($"duration"))
      .show(10,false)


    //    val playDf = sqlContext.read.parquet(pathForPlay2 :: pathForPlay3 :: Nil: _*)
    //      .filter($"date" between("2016-01-01", "2016-12-31"))
    //      .filter("event in ('startplay', 'playview')")
    //      .distinct
    //
    //    playDf.groupBy($"month").agg(count($"userId")).show(100, false)
    //    /* live and play */
    //    val liveAndPlayMonthAgg = liveDf.as("l").join(playDf.as("p"),
    //      $"l.month" === $"p.month" && $"l.userId" === $"p.userId")
    //      .groupBy($"l.month")
    //      .agg(count($"l.userId").as("uv"))
    //
    //    /* live not play */
    //
    //    val liveMonthAgg = liveDf.groupBy($"month").agg(count($"userId").as("uv"))
    //
    //    val playMonthAgg = playDf.groupBy($"month").agg(count($"userId").as("uv"))
    //
    //    val liveNotPlayMonthAgg = liveMonthAgg.as("l").join(liveAndPlayMonthAgg.as("j"), $"l.month" === $"j.month")
    //      .withColumn("liveNotPlay", $"l.uv" - $"j.uv")
    //      .select($"l.month", $"l.uv", $"liveNotPlay")
    //
    //    val playNotLiveMonthAgg = playMonthAgg.as("p").join(liveAndPlayMonthAgg.as("j"), $"p.month" === $"j.month")
    //      .withColumn("playNotLive", $"p.uv" - $"j.uv")
    //      .select($"p.month", $"p.uv", $"playNotLive")
    //
    //    liveAndPlayMonthAgg.show(100, false)
    //
    //    liveNotPlayMonthAgg.show(100, false)
    //
    //    playNotLiveMonthAgg.show(100, false)

    //    df.filter($"event" === "startplay")
    //      .groupBy(month($"date").as("month"))
    //      .agg(countDistinct($"userId").as("uv"))
    //      .show(100, false)

    //    liveDf.agg(countDistinct($"userId"))
    //
    //    val duration = liveDf
    //      .agg(sum($"duration").as("duration"))
    //      .withColumn("dur", $"duration" / 366)
    //
    //    duration
    //
    //    val userNum = liveDf.filter($"event" === "startplay")
    //      .distinct.count


  }
}
