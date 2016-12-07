package com.moretv.bi.report.common

import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 10/29/16.
  */
trait CommonUtil {

  def read(loadPath:String):DataFrame

  def filter:DataFrame

  def select:DataFrame

  def agg:DataFrame



}
