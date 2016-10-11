package com.moretv.bi.report.medusa.util.udf

/**
 * Created by Administrator on 2016/5/19.
 */
object InfoTransform {

  /**
   * 用于处理moretv的collect的event信息，使其同步与medusa的collect日志中的event
   */
  def transformEventInEvaluate(event:String)={
    var result = ""
    event match {
      case "up" => result = "ok"
      case "down" => result = "cancel"
      case _ => result = "error"
    }
    result
  }
}
