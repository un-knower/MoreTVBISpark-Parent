package com.moretv.bi.report.medusa.membership

import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by xia.jun on 2017/11/17.
  * 统计每个连续包月用户的月留存情况
  * 主要包含两种情况：1、第一次购买连续包月的用户，在后续的月留存情况；2、用户取消连续包月后，又重新购买了连续包月的用户在后续的月留存情况
  */
object ContinuityMonthRetention extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

      }

      case None => {println("参数有误！")}
    }

  }

}
