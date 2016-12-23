package com.moretv.bi.activity

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by zhangyu on 16/8/6.
  * 专用于统计活动的相关数据,每次需修改活动的上线时间onLineDay
  */

@deprecated
object OlympicActivityTest extends BaseClass {

  def main(args: Array[String]) {
<<<<<<< HEAD
    ModuleClass.executor(OlympicActivityTest, args)
=======
    ModuleClass.executor(this,args)
>>>>>>> 2bcd4a3b120e0bbd26d9f2184a052e2d7b62aa30
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        val calendar = Calendar.getInstance()
        val startDay = p.startDate
        calendar.setTime(DateFormatUtils.readFormat.parse(startDay))

        //活动上线时间,用于构造日期集合,如活动上线时间为0607,则该值为0608
        //本次活动9月8日上线
        val onLineDay = "20160909"

        (0 until p.numOfDays).foreach(i => {
          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)
          val sqlDay = DateFormatUtils.toDateCN(logDay, -1)

          //构造日期集合
          var logDaySet = logDay
          var activeDay = logDay
          val calendarTemp = Calendar.getInstance()
          calendarTemp.setTime(DateFormatUtils.readFormat.parse(logDay))
          while (activeDay != onLineDay) {
            calendarTemp.add(Calendar.DAY_OF_MONTH, -1)
            activeDay = DateFormatUtils.readFormat.format(calendarTemp.getTime)
            logDaySet += s",${activeDay}"
          }


          //各页面的浏览人数/次数(区分页面来源)(event = view,group by activityId,logtype,page,from)
          //tablename: activity_pageview_singleday(day,activityId,logtype,page,source,user_num,access_num)

          val logPathPageview = s"/log/activity/parquet/$logDay/pageview"
          sqlContext.read.load(logPathPageview).registerTempTable("log_data_pageview")

          val pageViewSingleDay = sqlContext.sql("select activityId,logType,page,`from`,count(distinct userId), " +
            "count(userId) from log_data_pageview where event = 'view' " +
            "group by activityId, logType, page, `from`").map(e => {
            val pageViewId = fromEngToChi(e.getString(0))
            val pageViewLogtype = fromEngToChi(e.getString(1))
            val pageViewPage = fromEngToChi(e.getString(2))
            val pageViewSource = fromEngToChi(e.getString(3))
            val pageViewUserNum = e.getLong(4)
            val pageViewAccessNum = e.getLong(5)
            (pageViewId, pageViewLogtype, pageViewPage, pageViewSource, pageViewUserNum, pageViewAccessNum)
          })

          if (p.deleteOld) {
            val pageViewSingleDayDel = "delete from activity_pageview_singleday where day = ?"
            util.delete(pageViewSingleDayDel, sqlDay)
          }

          val pageViewSingleDayInsert = "insert into activity_pageview_singleday(day,activityId,logtype,page,source,user_num,access_num) values(?,?,?,?,?,?,?)"
          pageViewSingleDay.collect().foreach(e => {
            util.insert(pageViewSingleDayInsert, sqlDay, e._1, e._2, e._3, e._4, e._5, e._6)
          })


          //各页面的浏览总人数/总次数(区分页面来源)
          //table_name:activity_pageview_dataset

          val logSetPathPageView = s"/log/activity/parquet/{$logDaySet}/pageview"
          //println(logSetPathPageView)
          sqlContext.read.load(logSetPathPageView).registerTempTable("log_dataset_pageview")

          val pageViewSetDay = sqlContext.sql("select activityId,logType,page,`from`,count(distinct userId), " +
            "count(userId) from log_dataset_pageview where event = 'view' " +
            "group by activityId, logType, page, `from`").map(e => {
            val pageViewSetId = fromEngToChi(e.getString(0))
            val pageViewSetLogType = fromEngToChi(e.getString(1))
            val pageViewSetPage = fromEngToChi(e.getString(2))
            val pageViewSetSource = fromEngToChi(e.getString(3))
            val pageViewSetUserNum = e.getLong(4)
            val pageViewSetAccessNum = e.getLong(5)
            (pageViewSetId, pageViewSetLogType, pageViewSetPage, pageViewSetSource, pageViewSetUserNum, pageViewSetAccessNum)
          })

          if (p.deleteOld) {
            val pageViewSetDayDel = "delete from activity_pageview_dataset where day = ?"
            util.delete(pageViewSetDayDel, sqlDay)
          }

          val pageViewSetDayInsert = "insert into activity_pageview_dataset(day,activityId,logtype,page,source,user_num,access_num) values(?,?,?,?,?,?,?)"
          pageViewSetDay.collect.foreach(e => {
            util.insert(pageViewSetDayInsert, sqlDay, e._1, e._2, e._3, e._4, e._5, e._6)
          })

          //各页面的其他行为人数/次数(不区分页面来源)(event != view,group by activityId,logtype,page,event)
          //activity_operation_singleday(day,activityId,logtype,page,event,user_num,access_num)

          val logPathOperation = s"/log/activity/parquet/$logDay/operation"
          sqlContext.read.load(logPathOperation).registerTempTable("log_data_operation")

          val operationSingleDay = sqlContext.sql("select activityId,logType,page,event,count(distinct userId), " +
            "count(userId) from log_data_operation where event != 'view' " +
            "group by activityId, logType, page, event").map(e => {
            val operationId = fromEngToChi(e.getString(0))
            val operationLogType = fromEngToChi(e.getString(1))
            val operationPage = fromEngToChi(e.getString(2))
            val operationEvent = fromEngToChi(e.getString(3))
            val operationUserNum = e.getLong(4)
            val operationAccessNum = e.getLong(5)
            (operationId, operationLogType, operationPage, operationEvent, operationUserNum, operationAccessNum)
          })

          if (p.deleteOld) {
            val operationSingleDayDel = "delete from activity_operation_singleday where day = ?"
            util.delete(operationSingleDayDel, sqlDay)
          }

          operationSingleDay.collect.foreach(e => {
            val operationSingleDayInsert = "insert into activity_operation_singleday(day,activityId,logtype,page,event,user_num,access_num) values(?,?,?,?,?,?,?)"
            util.insert(operationSingleDayInsert, sqlDay, e._1, e._2, e._3, e._4, e._5, e._6)
          })


          //各页面的其他行为总人数/次数(不区分页面来源)(event != view,group by activityId,logtype,page,event)
          //activity_operation_dataset(day,activityId,logtype,page,event,user_num,access_num)


          val logSetPathOperation = s"/log/activity/parquet/{$logDaySet}/operation"
          sqlContext.read.load(logSetPathOperation).registerTempTable("log_dataset_operation")

          val operationSetDay = sqlContext.sql("select activityId,logType,page,event,count(distinct userId), " +
            "count(userId) from log_dataset_operation where event != 'view' " +
            "group by activityId, logType, page, event").map(e => {
            val operationSetId = fromEngToChi(e.getString(0))
            val operationSetLogType = fromEngToChi(e.getString(1))
            val operationSetPage = fromEngToChi(e.getString(2))
            val operationSetEvent = fromEngToChi(e.getString(3))
            val operationSetUserNum = e.getLong(4)
            val operationSetAccessNum = e.getLong(5)
            (operationSetId, operationSetLogType, operationSetPage, operationSetEvent, operationSetUserNum, operationSetAccessNum)
          })

          if (p.deleteOld) {
            val operationSetDayDel = "delete from activity_operation_dataset where day = ?"
            util.delete(operationSetDayDel, sqlDay)
          }

          operationSetDay.collect.foreach(e => {
            val operationSetDayInsert = "insert into activity_operation_dataset(day,activityId,logtype,page,event,user_num,access_num) values(?,?,?,?,?,?,?)"
            util.insert(operationSetDayInsert, sqlDay, e._1, e._2, e._3, e._4, e._5, e._6)
          })

          //          //活动停留时长统计(表格里给出每个人每天的活动时长)
          //          //activity_duration(day,activityId,userId,total_time)
          //
          //          val logPathDuration = s"/log/activity/parquet/$logDay/operation"
          //          sqlContext.read.load(logPathDuration).registerTempTable("log_data_duration")
          //
          //          val duration = sqlContext.sql("select activityId,userId,sum(time) from log_data_duration " +
          //            "where logType = 'operation' and page = 'home' and event = 'back' " +
          //            "group by activityId, userId").map(e=>{
          //            val durationId = fromEngToChi(e.getString(0))
          //            var durationUserId = e.getString(1)
          //            if(durationUserId == "" || durationUserId.length() < 32){
          //              durationUserId = "未知"
          //            }
          //            val durationTotalTime = e.getDouble(2)
          //            (durationId,durationUserId,durationTotalTime)
          //          })
          //
          //          if(p.deleteOld) {
          //            val durationDel = "delete from activity_duration where day = ?"
          //            util.delete(durationDel,sqlDay)
          //          }
          //
          //          duration.collect.foreach(e=>{
          //            val durationInsert = "insert into activity_duration(day,activityId,userId,total_time) values(?,?,?,?)"
          //            util.insert(durationInsert,sqlDay,e._1,e._2,e._3)
          //          })


          calendar.add(Calendar.DAY_OF_MONTH, -1)

        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }


  def fromEngToChi(str: String): String = {
    if (str != "") {
      str match {
        //活动Id
        case "6jkl3erupr7o" => "微鲸*用户招募活动"
        //日志类型(logtype)
        case "pageview" => "浏览"
        case "operation" => "操作"
        // 活动各页面(page/from)
        case "tv" => "TV"
        case "mobile" => "手机"
        case "tb" => "贴吧"
        case "bbs" => "论坛"
        case "wb" => "微博"
        case "wxrd" => "微信阅读原文"
        case "wxmenu" => "微信固定入口"
        case "qq" => "微鲸官方群"
        //用户操作行为(event)
        case "view" => "浏览"
        case "submit" => "提交"
        //未知定义
        case "undesigned" => "未知"
        case "unknow" => "未知"
        case _ => "未知"
      }
    } else "未知"

  }
}
