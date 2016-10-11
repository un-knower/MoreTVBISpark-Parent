package com.moretv.bi.util

/**
 * Created by Will on 2015/9/12.
 */
object LogInputUtil {

  /**
   * 通过给定日期返回相应的loginlog的hdfs路径
   * @param day 允许格式为yyyyMMdd，其他格式会导致不能正确返回hdfs路径
   */
  def getLoginLog(day:String) = {
    s"/log/loginlog/loginlog.access.log_$day*"
  }
}
