package com.moretv.bi.temp.annual

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType

/**
  * Created by witnes on 1/9/17.
  */
object PlayViewStat extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }


  override def execute(args: Array[String]): Unit = {

    val weekOrWeekend = udf((w: String) => {
      w match {
        case "星期六" | "星期天" => "weekend"
        case _ => "weekday"
      }
    })

    val delta = udf((e: String, d: Long) => {
      (e, d) match {
        case ("playview", d) => d
        case _ => 0
      }
    })

    val q = sqlContext
    import q.implicits._

    val pathBeforeMarch =
      "/mbi/parquet/playview/{2015*,201601*,201602*,2016030*,2016031*,20160320,20160321}"
    val pathAfterMarch =
      "/log/medusaAndMoretvMerger/{2016*}/playview"

    sqlContext.read.parquet((pathBeforeMarch :: pathAfterMarch :: Nil): _*)
      .filter($"event" in("playview", "startplay"))
      .filter($"date" between("2015-01-01", "2016-12-31"))
      .select(year($"date").as("year"),
        hour(from_unixtime(unix_timestamp($"datetime", "yyyy-MM-dd hh:mm:ss") - delta($"event", $"duration"))).as("hour"),
        $"userId",
        weekOrWeekend(date_format($"date", "EEEE").as("dayOfWeek")).as("type"))
      .groupBy($"year", $"type", $"hour")
      .agg(countDistinct($"userId"))
      .show(100, false)
  }
}
