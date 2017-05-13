
package com.moretv.bi.util

import java.text.SimpleDateFormat
import java.util.Calendar


/**
  * Created by Will on 2014/12/26.
  */
object DateFormatUtils {

  /**
    * 英文月份缩写和中文格式月份的映射关系
    */
  private val monthMap = Map("Jan" -> "01",
    "Feb" -> "02",
    "Mar" -> "03",
    "Apr" -> "04",
    "May" -> "05",
    "Jun" -> "06",
    "Jul" -> "07",
    "Aug" -> "08",
    "Sep" -> "09",
    "Oct" -> "10",
    "Nov" -> "11",
    "Dec" -> "12")

  val cnFormat = new SimpleDateFormat("yyyy-MM-dd")
  val readFormat = new SimpleDateFormat("yyyyMMdd")
  val detailFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val minuteFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")


  def mergeDateWithTime(date: String, time: String) = {
    date + " " + time
  }


  /**
    * 将英文格式的月份转化为中文格式的月份
    *
    * @param enFormat 英文格式的日期字符串，dd/MM/yyyy
    * @return
    */
  def enFormat2CNFormat(enFormat: String) = {
    if (enFormat != null) {

      val day = enFormat.substring(0, 2)
      val month = enFormat.substring(3, 6)
      val year = enFormat.substring(7, 11)
      val time = enFormat.substring(12)
      monthMap.get(month) match {
        case Some(m) => year + "-" + m + "-" + day + " " + time
        case None => null
      }
    } else null

  }

  def en2CNDateFormat(enFormat: String) = {
    if (enFormat != null) {

      val day = enFormat.substring(0, 2)
      val month = enFormat.substring(3, 6)
      val year = enFormat.substring(7, 11)
      monthMap.get(month) match {
        case Some(m) => year + "-" + m + "-" + day
        case None => null
      }
    } else null

  }

  def toCNDateArray(enFormat: String) = {
    val datetime = if (enFormat != null) {

      val day = enFormat.substring(0, 2)
      val month = enFormat.substring(3, 6)
      val year = enFormat.substring(7, 11)
      val time = enFormat.substring(12)
      monthMap.get(month) match {
        case Some(m) => year + "-" + m + "-" + day + " " + time
        case None => null
      }
    } else null
    if (datetime != null) Array(datetime.substring(0, 10), datetime) else Array("", "")

  }

  def getDateCN(offset: Int = 0) = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, offset)
    cnFormat.format(cal.getTime)
  }

  def toDateCN(dateStr: String, offset: Int = 0) = {
    val date = readFormat.parse(dateStr)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_MONTH, offset)
    cnFormat.format(cal.getTime)
  }

  def enDateAdd(dateStr: String, offset: Int) = {
    val date = readFormat.parse(dateStr)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_MONTH, offset)
    readFormat.format(cal.getTime)
  }

  /**
    * 判断日期是否为周日
    *
    * @param cal
    * @return
    */
  def isSunday(cal: Calendar): Boolean = {
    cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY
  }

  /**
    * 判断日期是否为周一
    *
    * @param cal
    * @return
    */
  def isMonday(cal: Calendar): Boolean = {
    cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY
  }

  /**
    * 当日期为周日时，获取当周的日期范围
    *
    * @param cal
    * @return String: 形式yyyy-mm-dd~yyyy-mm-dd
    */
  def getWeekCN(cal: Calendar): String = {
    val date = cal.clone().asInstanceOf[Calendar]
    val end = getDateCN2(date, 0)
    val start = getDateCN2(date, -6)
    start + "~" + end
  }

  def getDateCN2(cal: Calendar, offset: Int = 0) = {
    cal.add(Calendar.DAY_OF_MONTH, offset)
    cnFormat.format(cal.getTime)
  }

  /**
    * 取当前日期所在的周的每一天
    *
    * @param offset 当前日期的偏移量
    * @return 返回的是传入日期day所在自然周的日期，自然周是指周一到周日
    */
  def getInputPathsWeek(day: String, offset: Int) = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(day))
    //cal.add(Calendar.WEEK_OF_MONTH, -1)
    cal.add(Calendar.WEEK_OF_YEAR, -1+offset)
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
