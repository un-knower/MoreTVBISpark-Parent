package com.moretv.bi.report.medusa.channeAndPrograma.mv

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DimensionTypes
import com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.SingerPlayStat.{MEDUSA_DIMENSION}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 3/31/17
  */
object MvHomePageEntranceForm {


  private var contentType: String = _

  /**
    * page_entrance_id, page_code , page_name, area_code, area_name, location_code, location_name
    */
  private var dimPageEntrance: DataFrame = _


  def getDimPageEntrance(mp: Map[String, String])(implicit sqLContext: SQLContext): Unit = {

    dimPageEntrance = DataIO.getDataFrameOps.getDimensionDF(sqLContext, mp,
      MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE
    )

  }

  /**
    * page_entrance_id
    * @param contentType
    * @param mp
    * @param rawOds
    * @param sqlContext
    * @throws java.lang.Exception
    * @return
    */
  @throws(classOf[Exception])
  def MvHomeEntranceIdFrom(contentType: String, mp: Map[String, String], rawOds: DataFrame)
                          (implicit sqlContext: SQLContext): DataFrame = {

    val sq = sqlContext
    import sq.implicits._

    val getMvHomePageLocUdf = udf[(String, String, String), String](getMvHomePageLoc)

    if (dimPageEntrance == null) {
      getDimPageEntrance(mp)
    }

    val odsDf =rawOds.withColumn("loc", getMvHomePageLocUdf($"pathMain"))
      .withColumn("page_code", $"loc._1")
      .withColumn("area_code", $"loc._2")
      .withColumn("location_code", $"loc._3").as("r")

    val ods1 =odsDf.filter($"location_code".isNotNull)
      .join(
        dimPageEntrance.filter($"location_code".isNotNull).as("d"),
        "page_code" :: "area_code" :: "location_code" :: Nil, "left_outer"
      )
      .drop($"page_code").drop($"area_code").drop($"location_code")
      .select($"r.*",$"d.page_entrance_id")

    val ods2 = odsDf.filter(!$"location_code".isNotNull).drop($"location_code")
      .join(
        dimPageEntrance.filter(!$"location_code".isNotNull).as("d"),
        "page_code" :: "area_code" :: Nil, "left_outer"
      )
      .drop($"page_code").drop($"area_code").drop($"location_code")
      .select(ods1.schema.fieldNames.map(col(_)): _*)

    ods1.unionAll(ods2)
  }


  def containsDigit(s: String): Boolean = {

    s.toCharArray.foreach(c => {
      if (Character.isDigit(c)) {
        return true
      }
    })
    return false
  }



  def getMvHomePageLoc(pathMain: String): (String, String, String) = {

    if(pathMain == null){
      return  ("","","")
    }

    val path = if(pathMain.contains("mv-search")){
      pathMain.replaceAll("mv-search","mv*function*search")
    }else{
      pathMain
    }

    val mvHomePages = path.split("\\-").filter(_.contains("mv*"))

    val mvHomePageAreas = if ( mvHomePages.length > 0){ mvHomePages.head.split("\\*") } else {Array.empty[String]}

    if (mvHomePageAreas.length <= 2) {
      return ("","","")
    }

    if(mvHomePageAreas(2).contains("_") && containsDigit(mvHomePageAreas(2))){
      ("mv",mvHomePageAreas(1),mvHomePageAreas(2).split("\\_")(0))
    }
    else if(containsDigit(mvHomePageAreas(2))){
      ("mv",mvHomePageAreas(1),null)
    }
    else{
      ("mv", mvHomePageAreas(1), mvHomePageAreas(2))
    }

  }


  val getMvHomePageLocUdf = udf[(String, String, String), String](getMvHomePageLoc)

}
