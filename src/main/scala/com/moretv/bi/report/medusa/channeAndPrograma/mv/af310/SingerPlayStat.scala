package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

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

  private val SingerBug = Map(
    "12hjefu9stkl"->"窦唯",
    "7ors1bjmqtij"->"杨宗纬",
  "7ors2dik5gtu"->"谭咏麟",
  "7ors34fhgifh"->"齐秦",
  "7ors6k6lkmgh"->"任贤齐",
  "7ors7ojm6kd4"->"张信哲",
  "7orsefhje4fg"->"华晨宇",
  "7orsij3fef2d"->"许巍",
  "7orsjlg61bxy"->"张杰",
  "7orsk76l5h5g"->"周华健",
  "7orskm6l9xl7"->"张敬轩",
  "7orsln5i9w5g"->"李克勤",
  "7orsmo3f3fef"->"李宗盛",
  "7orsnpiki68q"->"张震岳",
  "7orso84gw0ef"->"张学友",
  "7orssu3fk7qs"->"朴树",
  "7orsuvjmruru"->"光良",
  "7orsvxhjc3gh"->"李健",
  "7orsw05ifhac"->"刘德华",
  "7ovw1b6lf53f"->"迪克牛仔",
  "ab1cbcghv0ru"->"杜德伟",
  "c3x02def1bv0"->"周传雄",
  "deu9efnoop8r"->"汪苏泷",
  "o83e45g6moef"->"霍尊",
  "o83e4ghju9ef"->"黄品源",
  "o83e5hikijvw"->"刀郎",
  "o83e8r5imom7"->"张智霖",
  "o83eacfhqrbd"->"麦浚龙",
  "o83ebd3fij7n"->"李圣杰",
  "o83efg6lgifg"->"品冠",
  "o83egi3ft9m7"->"曹格",
  "o83eh6hjtvc3"->"潘玮柏",
  "o83estg6c3uv"->"薛之谦",
  "o83et9e5kma2"->"赵传",
  "o83eu9ika1su"->"郑伊健")

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

    sqlContext.udf.register("singerNameRevise", singerNameRevise _)

    if (deleteOld) {
      util.delete(deleteSql, readDate)
    }

    // (0 until fields.length - 1).map(r.get(_)): _*

    agg.toDF("singer_id","singer_name","entrance","entrance_vv","entrance_uv","entrance_duration",
      "total_vv","total_uv","total_duration").registerTempTable("tmp_log")

    val result = sqlContext.sql(
      """
        |select singer_id,singerNameRevise(singer_id,singer_name), entrance, entrance_vv, entrance_uv, entrance_duration,
        |total_vv,total_uv,total_duration
        |from tmp_log
      """.stripMargin)

    result.collect().foreach(r => {
      util.insert(insertSql, readDate,
        r.get(0), r.get(1),
        r.get(2), r.get(3), r.get(4),
        r.get(5), r.get(6), r.get(7), r.get(8)
      )

    })


  }

  def singerNameRevise(sid:String, name:String):String = {
    if(name==null){
      SingerBug.get(sid) match {
        case Some(p) => p
        case _ => sid
      }
    }else name
  }

}

