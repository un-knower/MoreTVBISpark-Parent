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

  val leisureLife = "漫生活"

  val others = "其它"


  val Live_First_Category = List(
    freshEntertainment,
    liveView,
    blackTech,
    eSports,
    leisureLife
  )



}
