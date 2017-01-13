package com.moretv.bi.temp.annual

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import org.apache.spark.sql.functions._

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


    val df = sqlContext.read.parquet(pathForLive2 :: pathForLive3 :: Nil: _*)
      .filter($"date" between("2016-01-01", "2016-12-31"))
      .filter($"duration" between(10, 36000))
      .filter($"liveType" === "live")
      .select($"date", $"userId", $"duration", $"event")

    //    df.filter($"event" === "startplay")
    //      .groupBy(month($"date").as("month"))
    //      .agg(countDistinct($"userId").as("uv"))
    //      .show(100, false)

    df.agg(countDistinct($"userId"))

    val duration = df
      .agg(sum($"duration").as("duration"))
      .withColumn("dur", $"duration" / 366 )

    duration

    val userNum = df.filter($"event" === "startplay")
      .distinct.count


  }
}
