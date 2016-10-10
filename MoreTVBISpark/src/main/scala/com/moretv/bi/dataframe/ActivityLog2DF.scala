package com.moretv.bi.dataframe

import com.moretv.bi.constant.Activity._
import com.moretv.bi.util.{SparkSetting, DateFormatUtils, LogUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/7/22.
 */

object ActivityLogTransformation extends SparkSetting{

//  def main(args: Array[String]) {
//    val inputDate = args(0)
//    val inputPath = LOG_PATH + inputDate + "*"
//    val outputPath = JSON_PATH+"/activity_"+DateFormatUtils.getDateCN(-offset)+".json"
//    implicit val sc = new SparkContext(config)
//
//
//  }
//
//  def transform(inputPath: String,outputPath: String)(implicit sc:SparkContext) = {
//    val jsonPath = JSON_PATH+"/activity_"+DateFormatUtils.getDateCN(-offset)+".json"
//
//    val sqlContext = new SQLContext(sc)
//    val logRdd = sc.textFile(inputPath).map(log => LogUtils.log2json(log)).
//    filter(json => json != null && json.opt(ACTIVITY_ID) != null).map(_.toString)
//    logRdd.saveAsTextFile(jsonPath)
//  }
}
