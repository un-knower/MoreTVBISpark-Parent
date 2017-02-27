package com.moretv.bi.temp

import com.moretv.bi.report.medusa.liveCastStat.FuncForLive

/**
  * Created by xiajun on 2017/2/26.
  */
object TimeTest {
  def main(args: Array[String]): Unit = {
    val info = FuncForLive.periodFillingWithStartEnd(12,13,12,45)
    info.foreach(println)
  }

}
