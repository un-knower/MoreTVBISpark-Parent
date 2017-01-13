package com.moretv.bi.report.medusa.liveCastStat

import org.apache.spark.sql.functions._

/**
  * Created by witnes on 1/12/17.
  */
object LiveSationTree {

  val freshEntertainment = "潮娱乐".r

  val liveView = "看现场".r

  val blackTech = "黑科技".r

  val eSports = "电竞风".r

  val leisureLife = "漫生活".r

  val others = "其它"

  val re = s"(${live_First_Category_Str})".r


  val categoryMatchUdf = udf(categoryMatch)

  val categoryMatch = (s: String) => {
    re findFirstMatchIn s match {
      case Some(p) => {
        if (blackTech.pattern.matcher(p.group(1)).matches) {
          blackTech.toString
        }
        if (eSports.pattern.matcher(p.group(1)).matches) {
          eSports.toString
        }
        if (leisureLife.pattern.matcher(p.group(1)).matches) {
          leisureLife.toString
        }
        if (freshEntertainment.pattern.matcher(p.group(1)).matches) {
          freshEntertainment.toString
        }
        if (liveView.pattern.matcher(p.group(1)).matches) {
          liveView.toString
        }
        else {
          others
        }
      }
      case None => {
        others
      }
    }
  }

  val Live_First_Category = List(
    freshEntertainment.toString,
    liveView.toString,
    blackTech.toString,
    eSports.toString,
    leisureLife.toString
  )

  val live_First_Category_Str = Live_First_Category.mkString("|")
}
