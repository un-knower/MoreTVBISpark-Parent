package com.moretv.bi.temp.annual

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 1/5/17.
  */
object LiveMonthCityDist extends BaseClass {

  private val tableName = "moretv_live_city_dist"

  private val fields = "day,city,uv,durationperuv"

  private val insertSql = s"insert into $tableName(day,city,uv,durationperuv)values(?,?,?,?)"


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    val q = sqlContext
    import q.implicits._

    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

    val getCity = udf((e: String) => IPLocationDataUtil.getCity(e))

    val path = "/log/medusaAndMoretvMerger/{201612*,20170101}/live"

    val df = sqlContext.read.parquet(path)
      .filter($"date" between("2016-12-01", "2016-12-31"))
      .filter($"liveType" === "live")
      .select($"event", $"duration", $"date", $"userId", getCity($"ip").as("city"))


    val uvDf = df.filter($"event" === "startplay")
      .groupBy("date", "city")
      .agg(countDistinct($"userId").as("uv"))

    val durationDf = df.filter($"event" === "switchchannel" && $"duration".between(1, 36000))
      .groupBy("date", "city")
      .agg(sum("duration").as("duration"))

    val resultDataSet = uvDf.as("u")
      .join(durationDf.as("d"), $"u.date" === $"d.date" && $"u.city" === $"d.city")
      .select($"u.date", $"u.city", $"u.uv", round($"d.duration" / $"u.uv", 3))
      .collect

    resultDataSet.foreach(w => {
      util.insert(insertSql, w.getString(0), w.getString(1), w.getLong(2), w.getDouble(3))
    })



  }
}
