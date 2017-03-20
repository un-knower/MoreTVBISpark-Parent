package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchEntranceResultStat._
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 3/17/17.
  *
  * TopN singers facet 's play stat and its entrance dist
  */
object SingerPlayStat extends BaseClass {


  private val contentType = "mv"

  private val sidDimPath = "/data_warehouse/dw_dimensions/dim_medusa_singer"

  private val postitonDimPath = "/data_warehouse/dw_dimensions/dim_medusa_position"


  private val sidMap = Map(
    "sid" -> col("singer_id"),
    "name" -> col("singer_name")
  )

  private val positionMap = Map(
    "entrance_code" -> col("position_code"),
    "entrance_title" -> col("position_title"),
    "entrance_content_type" -> col("position_content_type")
  )

  private val topNum = 200

  private val tableName = ""

  private val fields = Array(
    "day",
    "content_type",
    "sid_type",
    "sid",
    "name",
    "entrance_code",
    "entrance_title",
    "entrance_content_type",
    "entrance_vv",
    "entrance_uv",
    "total_vv",
    "total_uv"
  )


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val sq = sqlContext
        import sq.implicits._

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal = Calendar.getInstance

        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))


        val entranceFromUdf = udf[Seq[String], String, String, Boolean](entranceFrom)

        val isRecommendedUdf = udf[Boolean, String](isRecommended)


        // load Dim here

        val sidDim = sqlContext.read.parquet(sidDimPath)
          .select(
            sidMap.getOrElse("sid", col("")).as("sid"),
            sidMap.getOrElse("name", col("")).as("name")
          )
          .cache()

        val positionDim = sqlContext.read.parquet(postitonDimPath)
          .select(
            positionMap.getOrElse("entrance_code", col("")).as("entrance_code"),
            positionMap.getOrElse("entrance_title", col("")).as("entrance_title"),
            positionMap.getOrElse("entrance_content_type", col("")).as("entrance_content_type")
          )


        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)


          // load Fact here

          val df = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.PLAY, loadDate)

            .filter($"date" === sqlDate && $"contentType" === contentType)
            .withColumn("entrance_code",
              explode(
                entranceFromUdf($"contentType", explode(split($"pathMain", "-")), isRecommendedUdf($"recommendType"))
              )
            )
            .withColumnRenamed("singerSid", "sid")

            .filter($"sid".isNotNull)

            .withColumn("sid_type", lit("singer"))
            .withColumnRenamed("contentType", "content_type")



          // top sid selected
          val sidWithTotalStatDf =

            df.groupBy($"sid")
              .agg(count($"userId").as("total_vv"), countDistinct($"userId").as("total_uv"))
              .orderBy($"vv".desc)
              .limit(topNum)


          //selected sids are distributed over different entrance

          df.join(sidWithTotalStatDf, $"sid")
            .groupBy($"entrance_code", $"sid")
            .agg(count($"userId").as("entrance_vv"), countDistinct($"userId").as("entrance_uv"))
            .join(sidDim, "sid" :: "contentType" :: Nil)
            .join(positionDim, "entrance_code" :: Nil, "left_outer")
            .select(fields.map(col(_)): _*)
            .show(100,false)

        })


      }
      case None => {


      }
    }

  }

  /**
    * In general, page contains many parts ,which means it holds different classifications.
    *
    * @param pagePath
    * @param isRecommended
    */
  def entranceFrom(contentType: String, pagePath: String, isRecommended: Boolean): Seq[String] = {

    /**
      * if the contentType is in the last location of pageLocation :
      * then this page level is the upstream of this contentType homepage.
      *
      * else if the contentType is in the first location of pageLocation :
      * then this page level is exactly this contentType homepage.
      *
      * and if isRecommended is True:
      * then this entrance is in the recommendation area parts.
      * else
      * this entrance is in the contentType site tree.
      * and this entrance may have its own downstram pages.
      *
      */


    // home page , exp: home*classification*mv

    if (pagePath.endsWith(contentType)) {

      // get the location before this contentType flag

      val pathLocations = pagePath.split("\\*")

      val upstreamArea = pathLocations(pathLocations.length - 2)

      upstreamArea :: Nil

    }

    // contentType homepage

    else if (pagePath.startsWith(contentType)) {

      val pathLocations = pagePath.split("\\*")

      if (isRecommended) {

        // * *followed by sid , exp: mv*mvRecommendHomePage*34bdwxl7a19v

        pathLocations(1) :: Nil

      }
      else {

        // * * not followed by sid , exp: mv*function*site_hotsinger

        pathLocations.slice(1, pathLocations.length)

      }
    }

    else {
      // first level
      pagePath.split("\\*")(0) :: Nil
    }


  }


  def isRecommended(recommendType: String): Boolean = {

    if (recommendType != null && recommendType != "") {
      true
    }
    else {
      false
    }
  }
}
