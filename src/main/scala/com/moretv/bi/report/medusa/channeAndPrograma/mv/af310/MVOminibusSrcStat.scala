package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.channeAndPrograma.mv.MvStatModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 9/21/16.
  */

/**
  * 领域: MV [Medusa 3.1.0 及以上版本]
  * 对象: 精选集
  * 维度: 来源(入口), 天, 精选集(id & name)
  * 统计: pv, uv ,mean_duration
  * 数据源: play (注 medusa3.1.0 有新增字段[omnibusSid,omnibusName])
  * 提取特征: omnibusSid, omnibusName, userId, pathMain, duration
  * 输出: tbl[mv_ominibus_src_stat](day,entrance,ominibus_sid,ominibus_name,uv,pv,duration)
  */
object MVOminibusSrcStat extends MvStatModel {

  logType = LogTypes.PLAY

  pathPattern = MEDUSA

  tbl = "mv_ominibus_src_stat"

  fields = Array("day", "entrance", "ominibus_sid", "ominibus_name", "uv", "pv", "duration")


  override def aggUserStat(ods: DataFrame): DataFrame = {

    val q = sqlContext
    import q.implicits._


    val df = ods
      .filter($"page_entrance_id".isNotNull)
      .filter($"omnibusSid".isNotNull)

    df.filter($"event" === "startplay")
      .groupBy($"page_entrance_id", $"omnibusSid")
      .agg(count($"userId").as("vv"), countDistinct($"userId").as("uv"))

      .join(

        ods.filter($"event" !== "startplay").filter($"duration".between(1, 10800))
          .groupBy($"page_entrance_id", $"omnibusSid")
          .agg(sum($"duration").as("sum_duration")),

        "page_entrance_id" :: "omnibusSid" :: Nil,
        "inner"
      )
      .select($"page_entrance_id", $"omnibusSid".as("mv_topic_sid"), $"vv", $"uv",
        ($"sum_duration" / $"uv").cast("double").as("duration"))
  }


  override def joinStatWithDim(ods: DataFrame): DataFrame = {

    val q = sqlContext
    import q.implicits._

    val mvTopicDim = DataIO.getDataFrameOps.getDimensionDF(
      sc, Map[String, String](), MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_MV_TOPIC
    )

    val pageDim = DataIO.getDataFrameOps.getDimensionDF(
      sc, Map[String, String](), MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE
    )

    ods.join(pageDim, "page_entrance_id" :: Nil, "left_outer")
      .join(mvTopicDim, "mv_topic_sid" :: Nil, "left_outer")
      .select(
        $"page_name", $"area_name", $"location_name",
        $"mv_topic_sid",
        $"mv_topic_name",
        $"uv",
        $"vv",
        $"duration"
      )
  }

  override def outputHandle(
                             agg: DataFrame, deleteOld: Boolean, readDate: String,
                             util: MySqlOps, deleteSql: String, insertSql: String): Unit = {

    if (deleteOld) {
      util.delete(deleteSql, readDate)
    }


    agg.collect.foreach(r => {

      util.insert(insertSql,
        readDate, r.getString(0) + "-" + r.getString(1) + "-" + r.getString(2),
        r.getString(3), r.getString(4), r.getLong(5), r.getLong(6), r.getDouble(7)
      )

    }
    )
  }
}
