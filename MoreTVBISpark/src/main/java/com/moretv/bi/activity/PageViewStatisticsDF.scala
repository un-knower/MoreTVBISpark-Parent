package com.moretv.bi.activity

import java.util.Date

import com.moretv.bi.constant.Activity._
import com.moretv.bi.util.FileUtils._
import com.moretv.bi.util.{LogUtils, SparkSetting}
import org.apache.spark.SparkContext

/**
 * Created by Will on 2015/7/17.
 */
object PageViewStatisticsDF extends SparkSetting{

  val SEPARATOR = ","

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)

    implicit val sc = new SparkContext(config)
//    val activityDF = ActivityLog2DF.getActivityDF(inputPath,-1)
  }
}
