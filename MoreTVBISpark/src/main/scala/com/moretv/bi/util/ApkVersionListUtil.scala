package com.moretv.bi.util

/**
  * Created by Will on 2016/8/3.
  * 白名单和黑名单工具类，存放版本号的白名单和黑名单
  */
object ApkVersionListUtil {

  private val regex = "^MoreTV_[\\w\\.]+_\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(\\.\\d{1,3})?$".r

  def isBlack(version:String) = {
    if(version != null && version != ""){
      val flag = blackList.contains(version)
      if(flag) true
      else {
        regex findFirstIn version match {
          case Some(_) => false
          case None => true
        }
      }
    }else false
  }

  def isWhite(version:String) = !isBlack(version)

  private val blackList = List("_2.0.8")

}
