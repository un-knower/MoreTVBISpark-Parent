package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import org.apache.spark.sql.functions._
import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.channeAndPrograma.mv.MvStatModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 3/17/17.
  *
  * TopN singers facet 's play stat and its entrance dist
  */
object SingerPlayStat extends MvStatModel {

  val topNum = 200

  tbl = "medusa_play_singer_src"

  logType = LogTypes.PLAY

  pathPattern = MEDUSA

  fields = Array(
    "day", "singer_sid", "singer_name",
    "entrance_name", "entrance_vv", "entrance_uv", "entrance_duration",
    "total_vv", "total_uv", "total_duration"
  )


  override def aggUserStat(ods: DataFrame): DataFrame = {
    val q = sqlContext
    import q.implicits._

    ods.filter($"event" === "startplay")
      .groupBy($"singerSid")
      .agg(count($"userId").as("total_vv"), countDistinct($"userId").as("total_uv"))

      .join(

        ods.filter($"event" !== "startplay").filter($"duration".between(1, 10800))
          .groupBy($"singerSid")
          .agg(sum($"duration").as("duration_sum")),
        "singerSid" :: Nil, "inner"

      )

      .join(

        ods.filter($"event" === "startplay")
          .groupBy($"singerSid", $"page_entrance_id")
          .agg(count($"userId").as("entrance_vv"), countDistinct($"userId").as("entrance_uv"))

          .join(

            ods.filter($"event" !== "startplay").filter($"duration".between(1, 10800))
              .groupBy($"singerSid", $"page_entrance_id")
              .agg(sum($"duration").as("duration_entrance_sum")),
            "singerSid" :: "page_entrance_id" :: Nil, "inner"

          ),
        "singerSid" :: Nil, "left_outer"
      )
      .withColumnRenamed("singerSid", "singer_id")
      .withColumnRenamed("singerName", "singer_name")
      .withColumn("entrance_duration", ($"duration_entrance_sum" / $"entrance_uv").cast("double"))
      .withColumn("total_duration", ($"duration_sum" / $"total_uv").cast("double"))


  }

  override def joinStatWithDim(ods: DataFrame): DataFrame = {
    val q = sqlContext
    import q.implicits._

    val pageDim = DataIO.getDataFrameOps.getDimensionDF(
      sc, Map[String, String](), MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE
    )

    val singerDim = DataIO.getDataFrameOps.getDimensionDF(
      sc, Map[String, String](), MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_MV_SINGER
    )

    ods.join(pageDim, "page_entrance_id" :: Nil, "left_outer")
      .join(singerDim, "singer_id" :: Nil, "left_outer")
      .select(
        $"singer_id", $"singer_name",
        concat($"page_name", lit("-"), $"area_name", lit("-"), $"location_name"),
        $"entrance_vv", $"entrance_uv", $"entrance_duration",
        $"total_vv", $"total_uv", $"total_duration"
      )
  }

  override def outputHandle(agg: DataFrame, deleteOld: Boolean, readDate: String,
                            util: MySqlOps, deleteSql: String, insertSql: String): Unit = {

    if (deleteOld) {
      util.delete(deleteSql, readDate)
    }

    // (0 until fields.length - 1).map(r.get(_)): _*


    agg.collect().foreach(r => {
      util.insert(insertSql, readDate,
        r.get(0), r.get(1),
        r.get(2), r.get(3), r.get(4),
        r.get(5), r.get(6), r.get(7), r.get(8)
      )

    })


  }


}

