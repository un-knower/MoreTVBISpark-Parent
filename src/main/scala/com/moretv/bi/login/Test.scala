package com.moretv.bi.login

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by xiajun on 2017/3/4.
  */
object Test {

  def main(args: Array[String]): Unit = {
    getInputPaths(-14).foreach(println)
  }

  def getInputPaths(offset: Int) = {

    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.WEEK_OF_YEAR, offset)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    val monday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY)
    val tuesday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.WEDNESDAY)
    val wednesday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY)
    val thursday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY)
    val friday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY)
    val saturday = format.format(cal.getTime)
    cal.add(Calendar.WEEK_OF_YEAR, 1)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
    val sunday = format.format(cal.getTime)

    Array(monday, tuesday, wednesday, thursday, friday, saturday, sunday)

  }

}
