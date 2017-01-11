package com.moretv.bi.temp.annual

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._


/**
  * Created by witnes on 1/10/17.
  */
object PromotionChannelAU extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    val q = sqlContext
    import q.implicits._

    val loadPath = "/log/moretvloginlog/parquet/{2016*,20170101}/loginlog"

    sqlContext.read.parquet(loadPath)
      .filter($"date" between("2016-01-01", "2016-12-31"))
      .filter("promotionChannel is not null and length(promotionChannel) <50")
      .select($"mac", $"promotionChannel")
      .groupBy($"promotionChannel")
      .agg(countDistinct($"mac"))
      .show(500, false)

  }
}
