package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Double => JDouble, Long => JLong}
import java.util
import java.util.Calendar

import com.moretv.bi.report.medusa.channeAndPrograma.mv.MVRecommendPlay._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.report.medusa.channeAndPrograma.mv.MvStatModel
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, LiveCodeToNameUtils, ParamsParseUtil}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by witnes on 9/21/16.
  */

/**
  * 领域: MV [Medusa 3.1.0 及以上版本]
  * 对象: 精选集
  * 维度: 天
  * 数据源: play (注 medusa3.1.0 有新增字段)
  * 提取特征: omnibusSid, omnibusName, userId, pathMain,duration
  * 统计: pv, uv ,mean_duration
  */
object MVOminibusStat extends MvStatModel {

  logType = "play"

  pathPattern = "medusa"

  tbl = "mv_ominibus_stat"

  fields = Array("day", "ominibus_sid", "ominibus_name", "uv", "pv", "duration")


  override def aggUserStat(ods: DataFrame): DataFrame = {
    val q = sqlContext
    import q.implicits._
    import org.apache.spark.sql.functions._

    ods.filter($"event" === "startplay")
      .groupBy($"omnibusSid")
      .agg(count($"userId").as("pv"), countDistinct($"userId").as("uv"))

      .join(

        ods.filter($"event" !== "startplay").filter($"duration".between(1, 10800))
          .groupBy($"omnibusSid")
          .agg(sum($"duration").as("sum_duration"))
      )
      .select($"omnibusSid".as("mv_topic_sid"), $"pv", $"uv",
        ($"sum_duration" / $"uv").cast("double").as("duration"))

  }

  override def joinStatWithDim(ods: DataFrame): DataFrame = {

    val q = sqlContext
    import q.implicits._
    import org.apache.spark.sql.functions._

    val mvTopicDim = DataIO.getDataFrameOps.getDimensionDF(
      sc, Map[String, String](), MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_MV_TOPIC
    )

    ods.join(mvTopicDim, "mv_topic_sid" :: Nil, "left_outer")
      .select(
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
      println(readDate, r.getString(0), r.getString(1), r.getLong(2), r.getLong(3), r.getDouble(4), "!!!")
      util.insert(insertSql,
        readDate, r.getString(0), r.getString(1), r.getLong(2), r.getLong(3), r.getDouble(4)
      )
    }
    )
  }


}
