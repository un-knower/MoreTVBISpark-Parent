package com.moretv.bi.report.medusa.util.udf

/**
 * Created by Administrator on 2016/6/1.
 * 该类用于从datetime中抽取日期，小时等信息
 */
object TimeParser {

  def getHourFromDateTime(timestr:String)={
    val hour = timestr.substring(11,13)
    hour
  }
}
