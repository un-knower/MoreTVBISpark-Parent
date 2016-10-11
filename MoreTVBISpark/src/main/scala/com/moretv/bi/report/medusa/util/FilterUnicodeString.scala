package com.moretv.bi.report.medusa.util

/**
 * Created by Administrator on 2016/5/27.
 */
object FilterUnicodeString {
  def filterUnicodeString(str:String)={
    val regexp=""".*[\00a0-\00ff]""".r
    val result = regexp.findFirstIn(str)
    result match {
      case Some(p) => true
      case None => false
    }
  }

}
