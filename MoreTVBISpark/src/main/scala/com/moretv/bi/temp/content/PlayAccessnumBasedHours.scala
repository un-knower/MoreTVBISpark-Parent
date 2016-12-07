//package com.moretv.bi.temp.content
//
//import java.util.Calendar
//
//import cn.whaley.sdk.dataexchangeio.DataIO
//import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
//import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
//
///**
// * Created by zhangyu on 16/8/24.
// * 统计某contenttype下的观看量24小时分布
// * table_name : play_accessnum_based_hours
// * (id,day,contenttype,period,access_num)
// */
//object PlayAccessnumBasedHours extends BaseClass{
//
//  def main(args: Array[String]) {
//    ModuleClass.executor(PlayAccessnumBasedHours,args)
//  }
//
//  override def execute(args:Array[String]): Unit = {
//    ParamsParseUtil.parse(args) match{
//      case Some(p) => {
//
//        val util = DataIO.getMySqlOps("moretv_medusa_mysql")
//
//        val calendar = Calendar.getInstance()
//        val startDay = p.startDate
//        calendar.setTime(DateFormatUtils.readFormat.parse(startDay))
//
//        val contentType = p.contentType
//
//        (0 until p.numOfDays).foreach(i => {
//          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)
//          val sqlDay = DateFormatUtils.toDateCN(logDay,-1)
//
//          val logPath = s"/log/medusaAndMoretvMerger/$logDay/playview"
//          sqlContext.read.load(logPath).registerTempTable("log_data")
//
//          val rdd = sqlContext.sql("select substring(datetime,12,2) as period, " +
//            "count(userId) from log_data where contentType = 'hot' " +
//            "group by substring(datetime,12,2)").
//          map(row=>{
//            val period = row.getString(0)
//            val access_num = row.getLong(1)
//            (period,access_num)
//          })
//
//          if(p.deleteOld) {
//            val deleteSql = "delete from play_accessnum_based_hours where day = ?"
//            util.delete(deleteSql,sqlDay)
//          }
//
//          val insertSql = "insert into play_accessnum_based_hours(day,contenttype,period,access_num) values(?,?,?,?)"
//          rdd.collect().foreach(e => {
//            util.insert(insertSql,sqlDay,s"$contentType",e._1,e._2)
//          })
//
//          calendar.add(Calendar.DAY_OF_MONTH,-1)
//
//        })
//
//      }
//      case None => {
//        throw new RuntimeException("At least needs one param: startDate!")
//      }
//    }
//
//  }
//
//}
