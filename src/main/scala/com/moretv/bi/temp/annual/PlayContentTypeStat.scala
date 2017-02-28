package com.moretv.bi.temp.annual

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._


/**
  * Created by witnes on 1/17/17.
  */
object PlayContentTypeStat extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    val q = sqlContext
    import q.implicits._

    val pathBeforeMarch =
      "/mbi/parquet/playview/{2015*,201601*,201602*,2016030*,2016031*,20160320,20160321}"
    val pathAfterMarch =
      "/log/medusaAndMoretvMerger/{2016*,20170101}/playview"

    val df = sqlContext.read.parquet(pathAfterMarch :: pathBeforeMarch :: Nil: _*)
      .filter($"date".between("2015-01-01", "2016-12-31"))
      .select($"userId", year($"date").as("year"), $"contentType", $"duration", $"event")

    df.filter($"event".isin("startplay" :: "playview" :: Nil: _*))
      .groupBy($"year", $"contentType")
      .agg(count($"userId").as("vv"), countDistinct($"userId").as("uv"))
      .as("t1")
      .join(
        df.filter($"duration".isNotNull && $"duration".isInstanceOf[Int])
          .filter(!$"event".isin("startplay" :: Nil: _*))
          .filter($"duration".between(10, 36000))
          .groupBy($"year", $"contentType")
          .agg(sum($"duration").as("duration"))
          .as("t2"),
        $"t1.year" === $"t2.year" && $"t1.contentType" === $"t2.contentType")
      .select($"t1.year", $"t1.contentType", $"t1.uv", $"t1.vv", round($"t2.duration" / $"t1.uv", 3))
      .show(100, false)



  }


}
