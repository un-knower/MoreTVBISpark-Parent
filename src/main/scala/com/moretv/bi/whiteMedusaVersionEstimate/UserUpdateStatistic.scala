package com.moretv.bi.whiteMedusaVersionEstimate

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by zhu.bingxin on 2017/4/20.
  * 用户升级情况的统计
  * 按日新版本升级人数
  * 按日新版本累计人数 & 累计人数占比
  */
object UserUpdateStatistic extends BaseClass {

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  /**
    * this method do not complete.Sub class that extends BaseClass complete this method
    */
  override def execute(args: Array[String]): Unit = {

  }
}
