package com.moretv.bi.util

/**
 * Created by zhangyu on 17/11/30.
 * 构造参数配置类,从shell或配置中收集参数
 */
class ConfigureBuilder(val _sleepTime:Long,
                       val _retryNum:Int,val _calculateNum:Int){

  def retryNum = _retryNum
  def sleepTime = _sleepTime
  def calculateNum = _calculateNum


}
