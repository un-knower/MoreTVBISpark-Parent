package com.moretv.bi.util

import java.text.SimpleDateFormat

/**
 * Created by zhangyu on 17/11/29.
 */
object TimeUtil {

  /**
   * 向就近的分钟取整(向前取整)
   * @param time (pattern:yyyymmdd hh:MM:ss) -> (yyyymmdd hh:MM:00)
   */
  def floorMinute(time: String) = {
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time)
    val seconds = date.getTime
    val resSeconds = (seconds / 60000) * 60000
    val res = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    res.format(resSeconds)

  }

  /**
   * 向就近的分钟取整(向后取整)
   * @param time (pattern:yyyymmdd hh:MM:ss) -> (yyyymmdd hh:MM:00)
   */
  def ceilMinute(time: String) = {
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time)
    val seconds = date.getTime
    val resSeconds = (seconds / 60000 + 1) * 60000
    val res = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    res.format(resSeconds)
  }
}
