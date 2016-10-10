package com.moretv.bi.util.baseclasee

import java.io.{PrintWriter, StringWriter}

/**
 * 这个工具类的主要作用是转换异常为String类型
 * Created by admin on 16/8/10.
 */
object ExceptionUtils {

  /**
   * 将异常转换成string
   * @param e
   */
  def getErrorInfoFromException(e:Throwable)={
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    sw.toString
  }
}
