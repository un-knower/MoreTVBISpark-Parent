//package com.moretv.bi.activity
//
//import java.util.Calendar
//
//import cn.whaley.sdk.dataexchangeio.DataIO
//import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
//import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
//
//import scala.collection.mutable.ListBuffer
//
///**
// * Created by zhangyu on 16/9/2.
// * 专用于统计活动中某直播频道的直播数据,包括直播的UV/VV/人均观看次数/人均观看时长/在线最高人数及其时间
// *
// * 主要参数:startDate,sid,numOfDays,deleteOld,durationMax等
// *
// * 人数次数统计
// * table_name: live_uvvv_based_channelsid
// * (id, day, channnelsid,user_num,access_num,avg_num)
// *
// * 人均观看时长统计
// * table_name: live_avg_duration_based_channelsid
// * (id,day,channelsid,sum_duration,avg_duration)
// *
// * 每分钟在线人数统计
// * table_name: live_user_every_minute_based_channelsid
// * (id,day,channelSid,time,user_num)
// */
//
//object LiveStatisticsBasedChannelsid extends BaseClass {
//
//  def main(args: Array[String]) {
//    ModuleClass.executor(LiveStatisticsBasedChannelsid, args)
//  }
//
//  override def execute(args: Array[String]): Unit = {
//    ParamsParseUtil.parse(args) match {
//      case Some(p) => {
//        val util = DataIO.getMySqlOps("moretv_medusa_mysql")
//
//        val startDate = p.startDate
//        val channelSid = p.sid
//
//        //确定duration时长最小值最大值
//        val durationMin = 0
//        val durationMax = p.durationMax
//
//        val calendar = Calendar.getInstance()
//        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
//
//        (0 until p.numOfDays).foreach(x => {
//          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)
//          val sqlDay = DateFormatUtils.toDateCN(logDay, -1)
//          val logPath = s"/log/medusaAndMoretvMerger/$logDay/live"
//
//          sqlContext.read.load(logPath).registerTempTable("log_data")
//
//          //统计人数/次数/人均次数
//          val uvvvRdd = sqlContext.sql("select count(distinct userId),count(userId),count(userId)/count(distinct userId) " +
//            s"from log_data where channelSid = '$channelSid' and " +
//            s"event = 'startplay' and " +
//            s"liveType = 'live'").
//            map(row => {
//              val user_num = row.getLong(0)
//              val access_num = row.getLong(1)
//              val avg_num = row.getDouble(2)
//              (user_num, access_num, avg_num)
//            })
//
//          if(p.deleteOld){
//            val deleteSql1 = "delete from live_uvvv_based_channelsid where day = ?"
//            util.delete(deleteSql1,sqlDay)
//          }
//
//          uvvvRdd.collect().foreach(e=>{
//            val insertSql1 = "insert into live_uvvv_based_channelsid(day,channelsid,user_num,access_num,avg_num) values(?,?,?,?,?)"
//            util.insert(insertSql1,sqlDay,s"$channelSid",e._1,e._2,e._3)
//          })
//
//          println("successfully One!!")
//
//
//          //统计总时长/人均时长(s)
//          val avgDurationRdd = sqlContext.sql("select sum(duration),sum(duration)/count(distinct userId) " +
//            s"from log_data where channelSid = '$channelSid' and " +
//            s"event = 'switchchannel' and " +
//            s"liveType = 'live' and " +
//            s"duration between 0 and '$durationMax'").
//            map(row => {
//              val sum_duration = row.getLong(0)
//              val avg_duration = row.getDouble(1)
//              (sum_duration, avg_duration)
//            })
//
//          if(p.deleteOld){
//            val deleteSql2 = "delete from live_avg_duration_based_channelsid where day = ?"
//            util.delete(deleteSql2,sqlDay)
//          }
//
//          avgDurationRdd.collect.foreach(e=>{
//            val insertSql2 = "insert into live_avg_duration_based_channelsid(day,channelsid,sum_duration,avg_duration) values(?,?,?,?)"
//            util.insert(insertSql2,sqlDay,s"$channelSid",e._1,e._2)
//          })
//
//          println("successfully Two!!")
//
//
//          //统计每分钟在线的人数,采用分割每个人的duration到每分钟的方式统计
//
//          val usernumMinuteRdd = sqlContext.sql("select userId, duration,datetime from log_data " +
//            s"where channelSid = '$channelSid' and " +
//            s"event = 'switchchannel' and " +
//            s"liveType = 'live' and " +
//            s"duration between '$durationMin' and '$durationMax'").
//            map(e => (e.getString(0), e.getLong(1), e.getString(2))).
//            flatMap(e => (toMinite(e._1, e._2, e._3))).
//            distinct().countByKey()
//
//          if(p.deleteOld){
//            val deleteSql3 = "delete from live_user_every_minute_based_channelsid where day = ?"
//            util.delete(deleteSql3,sqlDay)
//          }
//
//          usernumMinuteRdd.foreach(e=>{
//            val insertSql3 = "insert into live_user_every_minute_based_channelsid(day,channelSid,time,user_num) values(?,?,?,?)"
//            util.insert(insertSql3,sqlDay,s"$channelSid",e._1,e._2)
//          })
//
//          println("successfully Three!!")
//
//          calendar.add(Calendar.DAY_OF_MONTH, -1)
//        })
//      }
//      case None => {
//        throw new RuntimeException("At least needs one param: startDate!")
//      }
//    }
//  }
//
//  //输入:每个人的播放时长和退出时间
//  //输出: 每个人在其播放期间的每一分钟均会输出一个tuple (分钟,userId),汇总为listbuffer
//  def toMinite(userId: String, duration: Long, datetime: String) = {
//    val result = new ListBuffer[(String, String)]()
//    val minute = Math.ceil(duration * 1.0 / 60).toInt
//    val cal = Calendar.getInstance()
//    cal.setTime(DateFormatUtils.detailFormat.parse(datetime))
//    (0 to minute).map(e => {
//      val currentMinute = DateFormatUtils.minuteFormat.format(cal.getTime)
//      result.+=((currentMinute, userId))
//
//      cal.add(Calendar.MINUTE, -1)
//    })
//    result
//  }
//}
