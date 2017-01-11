package com.moretv.bi.temp.annual

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 1/9/17.
  */
object ActiveUserStat extends BaseClass {

  def main(args: Array[String]) {

    ModuleClass.executor(this, args)
  }

  val weekOrWeekend = udf((w: String) => {
    w match {
      case "星期六" | "星期天" => "weekend"
      case _ => "weekday"
    }
  })

  override def execute(args: Array[String]): Unit = {

    val q = sqlContext
    import q.implicits._

    val path = "/log/moretvloginlog/parquet/{2016*,20170101}/loginlog"

    val path1 = "/log/moretvloginlog/parquet/{2015*,20160101}/loginlog"



//    sqlContext.read.parquet(path)
    //      .filter($"date" between("2016-01-01", "2016-12-31"))
    //      .agg(countDistinct($"mac"))
    //      .show(100, false)


        sqlContext.read.parquet(path1)
          .filter($"date" between("2015-01-01", "2015-12-31"))
          .groupBy($"mac")
          .agg(countDistinct($"date").as("days"))
          .groupBy($"days")
          .agg(countDistinct($"mac").as("user_num"))
          .show(500,false)

    //    sqlContext.read.parquet(path)
    //      .filter($"date" between("2016-01-01", "2016-12-31"))
    //      .select(date_format($"date", "EEEE").as("dayOfWeek"), hour($"datetime").as("hour"), $"mac")
    //      .withColumn("type", weekOrWeekend($"dayOfWeek"))
    //      .groupBy($"type", $"hour")
    //      .agg(countDistinct($"mac"))
    //      .show(100, false)

    //    sqlContext.read.parquet(path)
    //      .filter($"date" between("2016-01-01", "2016-12-31"))
    //      .select(date_format($"date", "EEEE").as("dayOfWeek"), minute($"datetime").as("minute"), $"mac")
    //      .withColumn("type", weekOrWeekend($"dayOfWeek"))
    //      .groupBy($"type", $"minute")
    //      .agg(countDistinct($"mac"))
    //      .show(100, false)

  }
}
