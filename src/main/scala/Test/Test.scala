package Test

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.DateFormatUtils

/**
  * Created by zhu.bingxin on 2017/5/11.
  */
object Test {


  def main(args: Array[String]): Unit = {


    getInputPathsWeek("20170430",0).foreach(println)

    val x = "方东"


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
    println(cal.getTime)
    //cal.add(Calendar.WEEK_OF_MONTH, -1)
    cal.add(Calendar.WEEK_OF_YEAR, -1+offset)
    println(cal.getTime)
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
