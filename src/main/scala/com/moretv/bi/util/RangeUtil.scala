package com.moretv.bi.util

/**
 * Created by Will on 2015/10/1.
 */
case class RangeUtil(val start:Int,val step:Int) {

  private var origin = start - step
  def reload(): Unit = {
    origin = start - step
  }
  def increase(): Unit ={
    origin = origin + step
  }
  def next = {
    increase()
    origin
  }
}
