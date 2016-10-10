//package com.moretv.bi.medusa.temp
//
//import java.util.Calendar
//
//import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
//
///**
//  * Created by witnes on 9/8/16.
//  */
//object RecommendTrend {
//
//
//  private val tableName = "medusa_temp_mibox_trend"
//
//
//
//  def main(args: Array):Unit ={
//
//
//  }
//
//
//  override def execute(args: Array[String]): Unit = {
//
//    ParamsParseUtil.parse(args) match {
//      case Some(p) => {
//
//        val util = new DBOperationUtils("medusa")
//        val cal = Calendar.getInstance
//        val startDate = p.startDate
//        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
//
//        (0 until p.numOfDays).foreach(i =>{
//          val date = DateFormatUtils.readFormat.format(cal.getTime)
//
//          cal.add(Calendar.DAY_OF_MONTH,-1)
//        })
//      }
//      case None => {
//        throw new RuntimeException("at least needs one param: startDate")
//      }
//    }
//  }
//
//}
