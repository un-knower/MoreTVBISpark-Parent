package com.moretv.bi.temp

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.report.medusa.liveCastStat.LiveChannelPlayStat._
import com.moretv.bi.temp.annual.LiveMonthCityDist._
import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 3/6/17.
  */
object IPAreaMapTst extends BaseClass {

  private val tableName = "moretv_play_city_dist"

  private val fields = "day,city,vv,uv"

  private val insertSql = s"insert into $tableName($fields)values(?,?,?,?)"


  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "120")

    ModuleClass.executor(this, args)


  }

  override def execute(args: Array[String]): Unit = {

    val q = sqlContext
    import q.implicits._

    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

    val pt = "/log/medusa/parquet/{20170101}/play"

    val getCity = udf((e: String) => IPLocationDataUtil.getCity(e))

    val getDistrict = udf((e: String) => IPLocationDataUtil.getDistrict(e))

    val tstDf = sqlContext.read.parquet(pt)
      .select($"ip", $"userId", $"date")
      .distinct
      .cache


    //
    val area3Sections = sqlContext.read.parquet("/data_warehouse/dw_dimensions/dim_web_location")

    val rslt1 = tstDf.withColumn("ip_section_1", split($"ip", "\\.")(0))
      .withColumn("ip_section_2", split($"ip", "\\.")(1))
      .withColumn("ip_section_3", split($"ip", "\\.")(2))
      .join(area3Sections, Array("ip_section_1", "ip_section_2", "ip_section_3"), "inner")
      .groupBy($"date", $"district")
      .agg(count($"userId").as("vv1"), countDistinct($"userId").as("uv1"))


    //
    val rslt2 = tstDf.withColumn("district", getDistrict($"ip"))
      .groupBy($"date", $"district")
      .agg(count($"userId").as("vv2"), countDistinct($"userId").as("uv2"))


    rslt1.join(rslt2, Array("date", "district"), "inner")
      .withColumn("uv_diff", ($"uv1" - $"uv2"))
      .withColumn("vv_diff", ($"vv1" - $"vv2"))
      .withColumn("uv_diff_r", ($"uv1" - $"uv2") / $"uv2")
      .withColumn("vv_diff_r", ($"vv1" - $"vv2") / $"vv2")
      .orderBy($"uv_diff_r".desc, $"date".asc)
      .show(300, false)

  }
}
