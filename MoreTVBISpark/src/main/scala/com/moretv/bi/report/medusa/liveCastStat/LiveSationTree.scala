package com.moretv.bi.report.medusa.liveCastStat

import org.apache.spark.sql.functions._


/**
  * Created by witnes on 1/12/17.
  */
object LiveSationTree {

  val freshEntertainment = "潮娱乐"

  val liveView = "看现场"

  val blackTech = "黑科技"

  val eSports = "电竞风"

  val leisureLife = "慢生活"

  val others = "其它"


  val categoryMatchUdf = udf(categoryMatch)

  val categoryMatch = (s: String) => {

    val re = s"(${live_First_Category_Str})".r

    re findFirstMatchIn s match {
      case Some(p) => p.group(1)
      case None => others
    }
  }

  val Live_First_Category = List(
    freshEntertainment,
    liveView,
    blackTech,
    eSports,
    leisureLife
  )

  val live_First_Category_Str = Live_First_Category.mkString("|")
}
