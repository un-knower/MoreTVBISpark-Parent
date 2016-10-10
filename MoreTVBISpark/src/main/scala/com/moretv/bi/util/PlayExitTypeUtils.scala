package com.moretv.bi.util

/**
  * Created by Will on 2016/1/25.
  */
object PlayExitTypeUtils {

  //新版VV日志的发行版本号，自此版本后，VV统计发生变化
  private val VERSION_NEW_VV = "2.4.5"

  def isVVLog(version:String) = {
    if(version.compareTo(VERSION_NEW_VV) <= 0) true else false
  }
}
