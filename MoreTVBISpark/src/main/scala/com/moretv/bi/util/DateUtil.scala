package com.moretv.bi.util

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ListBuffer

/**
 * Created by laishun on 15/9/11.
 */
trait DateUtil {
  def getDateRegion(flag:String,date:String)={
    val cal = Calendar.getInstance();
    val formatCN = new SimpleDateFormat("yyyyMMdd");
    cal.setTime(formatCN.parse(date));
    var dateList = new ListBuffer[String]()

    //根据不同的参数返回不同的时间区间
    if(flag == "week") {//上一周的时间区间
      val dayOfWeek = cal.get(Calendar.DAY_OF_WEEK)
      cal.add(Calendar.DATE, cal.getActualMinimum(Calendar.DAY_OF_WEEK)-dayOfWeek + 1)
      for(i <- 1 to 7){
        cal.add(Calendar.DATE,-1)
        dateList += formatCN.format(cal.getTime)
      }
    }else if(flag == "oneMonth"){//上个月的时间
      cal.add(Calendar.MONTH,-1)
      val year = cal.get(Calendar.YEAR)
      val month = cal.get(Calendar.MONTH)+1
      var tempTime = ""
      if(month >=1 && month <=9){
        tempTime=year +"0"+month+"*"
      }else if(month >= 10){
        tempTime=year +""+month+"*"
      }
      dateList += tempTime
    }else if(flag == "threeMonth"){//上三个月的时间
      for(i <- 1 to 3){
        cal.add(Calendar.MONTH,-1)
        val year = cal.get(Calendar.YEAR)
        val month = cal.get(Calendar.MONTH)+1
        var tempTime = ""
        if(month >=1 && month <=9){
          tempTime=year +"0"+month+"*"
        }else if(month >= 10){
          tempTime=year +""+month+"*"
        }
        dateList += tempTime
      }
    }
    dateList
  }

  def getWeekStartToEnd(dateStr:String)={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(dateStr))
    val dayOfweek = cal.get(Calendar.DAY_OF_WEEK)

    val First:Calendar = cal.clone().asInstanceOf[Calendar]
    First.add(Calendar.DATE,
      cal.getActualMinimum(Calendar.DAY_OF_WEEK)
        - dayOfweek + 1);
    val Last:Calendar = cal.clone().asInstanceOf[Calendar]
    Last.add(Calendar.DATE,
      cal.getActualMaximum(Calendar.DAY_OF_WEEK)
        - dayOfweek + 1);

    format.format(First.getTime()) + "~" + format.format(Last.getTime())
  }
}
