package com.moretv.bi.template.StatHandler

/**
  * Created by witnes on 11/3/16.
  */
trait StatBase {


  def dataSourceLoader


  def searchFieldsLoader


  def sqlExecutor


  def save


}
