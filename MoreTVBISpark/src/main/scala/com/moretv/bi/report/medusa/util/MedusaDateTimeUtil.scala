package com.moretv.bi.report.medusa.util

import org.joda.time.DateTime

/**
 * Created by Administrator on 2016/4/16.
 */
object MedusaDateTimeUtil {

  def getHourFromDateTime(dateTime:DateTime):Int={
    val hour = dateTime.getHourOfDay
    hour
  }
}
