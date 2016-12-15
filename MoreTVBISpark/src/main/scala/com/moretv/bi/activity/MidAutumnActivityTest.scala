package com.moretv.bi.activity

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by czw on 16/9/12.
  *  专用于统计活动的相关数据,每次需修改活动的上线时间onLineDay
  */
object MidAutumnActivityTest extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(MidAutumnActivityTest,args)
  }

  override def execute(args:Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        val calendar = Calendar.getInstance()
        val startDay = p.startDate
        calendar.setTime(DateFormatUtils.readFormat.parse(startDay))

        //活动上线时间,用于构造日期集合,保证其时间比当天早
        val onLineDay = "20160916"

        (0 until p.numOfDays).foreach(i=>{
          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)
          val sqlDay = DateFormatUtils.toDateCN(logDay,-1)

          //构造日期集合
          var logDaySet = logDay
          var activeDay = logDay
          val calendarTemp = Calendar.getInstance()
          calendarTemp.setTime(DateFormatUtils.readFormat.parse(logDay))
          while(activeDay != onLineDay){
            calendarTemp.add(Calendar.DAY_OF_MONTH,-1)
            activeDay = DateFormatUtils.readFormat.format(calendarTemp.getTime)
            logDaySet += s",${activeDay}"
          }

          //各页面的浏览人数/次数(区分页面来源)(event = view,group by activityId,page,source)
          //tablename: activity_pageview_singleday(day,activityId,page,source,user_num,access_num)

          val logPathPageview = s"/log/activity/parquet/$logDay/pageview"
          sqlContext.read.load(logPathPageview).registerTempTable("log_data_pageview")

          val pageViewSingleDay = sqlContext.sql("select activityId,page,source,count(distinct userId), " +
            "count(userId) from log_data_pageview where event = 'view'" +
            "group by activityId,page,source").map(e=>{
            val pageViewId = fromEngToChi(e.getString(0))
            val pageViewPage = fromEngToChi(e.getString(1))
            val pageViewSource=fromEngToChi(e.getString(2))
            val pageViewUserNum = e.getLong(3)
            val pageViewAccessNum = e.getLong(4)
            (pageViewId,pageViewPage,pageViewSource,pageViewUserNum,pageViewAccessNum)
          })

          if(p.deleteOld) {
            val pageViewSingleDayDel = "delete from midautumn_activity_pageview_singleday where day = ?"
            util.delete(pageViewSingleDayDel, sqlDay)
          }

          val pageViewSingleDayInsert = "insert into midautumn_activity_pageview_singleday(day,activityId,page,source,user_num,access_num) values(?,?,?,?,?,?)"
          pageViewSingleDay.collect().foreach(e=>{
            util.insert(pageViewSingleDayInsert,sqlDay,e._1,e._2,e._3,e._4,e._5)
          })


          //各页面的浏览总人数/总次数(区分页面来源)
          //table_name:activity_pageview_dataset

          val logSetPathPageView = s"/log/activity/parquet/{$logDaySet}/pageview"
          println(logSetPathPageView)
          sqlContext.read.load(logSetPathPageView).registerTempTable("log_dataset_pageview")

          val pageViewSetDay = sqlContext.sql("select activityId,page,source,count(distinct userId), " +
            "count(userId) from log_dataset_pageview where event = 'view'" +
            "group by activityId,page").map(e=>{
            val pageViewSetId = fromEngToChi(e.getString(0))
            val pageViewSetPage = fromEngToChi(e.getString(1))
            val pageViewSetSource=fromEngToChi(e.getString(2))
            val pageViewSetUserNum = e.getLong(3)
            val pageViewSetAccessNum = e.getLong(4)
            (pageViewSetId,pageViewSetPage,pageViewSetSource,pageViewSetUserNum,pageViewSetAccessNum)
          })

          if(p.deleteOld) {
            val pageViewSetDayDel = "delete from midautumn_activity_pageview_dataset where day = ?"
            util.delete(pageViewSetDayDel, sqlDay)
          }

          val pageViewSetDayInsert = "insert into midautumn_activity_pageview_dataset (day,activityId,page,source,user_num,access_num) values(?,?,?,?,?,?)"
          pageViewSetDay.collect.foreach(e=>{
            util.insert(pageViewSetDayInsert,sqlDay,e._1,e._2,e._3,e._4,e._5)
          })


          //各页面的其他行为人数/次数(区分页面来源)(event != view,group by activityId,page,source,event)
          //activity_operation_singleday(day,activityId,page,source,event,user_num,access_num)

          val logPathOperation = s"/log/activity/parquet/$logDay/operation"
          sqlContext.read.load(logPathOperation).registerTempTable("log_data_operation")

          val operationSingleDay = sqlContext.sql("select activityId,page,source,event,count(distinct userId), " +
            "count(userId) from log_data_operation where event != 'view' " +
            "group by activityId,page,source,event").map(e=>{
            val operationId = fromEngToChi(e.getString(0))
            val operationPage = fromEngToChi(e.getString(1))
            val operationSource=fromEngToChi(e.getString(2))
            val operationEvent = fromEngToChi(e.getString(3))
            val operationUserNum = e.getLong(4)
            val operationAccessNum = e.getLong(5)
            (operationId,operationPage,operationSource,operationEvent,operationUserNum,operationAccessNum)
          })

          if(p.deleteOld) {
            val operationSingleDayDel = "delete from midautumn_activity_operation_singleday where day = ?"
            util.delete(operationSingleDayDel,sqlDay)
          }

          operationSingleDay.collect.foreach(e=>{
            val operationSingleDayInsert = "insert into midautumn_activity_operation_singleday (day,activityId,page,source,event,user_num,access_num) values(?,?,?,?,?,?,?)"
            util.insert(operationSingleDayInsert,sqlDay,e._1,e._2,e._3,e._4,e._5,e._6)
          })


          //各页面的其他行为总人数/次数(区分页面来源)(event != view,group by activityId,page,source,event)
          //activity_operation_dataset(day,activityId,page,source,event,user_num,access_num)


          val logSetPathOperation = s"/log/activity/parquet/{$logDaySet}/operation"
          sqlContext.read.load(logSetPathOperation).registerTempTable("log_dataset_operation")

          val operationSetDay = sqlContext.sql("select activityId,page,source,event,count(distinct userId), " +
            "count(userId) from log_dataset_operation where event != 'view' " +
            "group by activityId,page,source,event").map(e=>{
            val operationSetId = fromEngToChi(e.getString(0))
            val operationSetPage = fromEngToChi(e.getString(1))
            val operationSetSource=fromEngToChi(e.getString(2))
            val operationSetEvent = fromEngToChi(e.getString(3))
            val operationSetUserNum = e.getLong(4)
            val operationSetAccessNum = e.getLong(5)
            (operationSetId,operationSetPage,operationSetSource,operationSetEvent,operationSetUserNum,operationSetAccessNum)
          })

          if(p.deleteOld) {
            val operationSetDayDel = "delete from midautumn_activity_operation_dataset where day = ?"
            util.delete(operationSetDayDel,sqlDay)
          }

          operationSetDay.collect.foreach(e=>{
            val operationSetDayInsert = "insert into midautumn_activity_operation_dataset (day,activityId,page,source,event,user_num,access_num) values(?,?,?,?,?,?,?)"
            util.insert(operationSetDayInsert,sqlDay,e._1,e._2,e._3,e._4,e._5,e._6)
          })

          /*
          //活动停留时长统计(表格里给出每个人每天的活动时长)
          //activity_duration(day,activityId,userId,total_time)

          val logPathDuration = s"/log/activity/parquet/$logDay/operation"
          sqlContext.read.load(logPathDuration).registerTempTable("log_data_duration")

          val duration = sqlContext.sql("select activityId,userId,sum(time) from log_data_duration " +
            "where logType = 'operation' and page = 'home' and event = 'back' " +
            "group by activityId, userId").map(e=>{
            val durationId = fromEngToChi(e.getString(0))
            var durationUserId = e.getString(1)
            if(durationUserId == "" || durationUserId.length() < 32){
              durationUserId = "未知"
            }
            val durationTotalTime = e.getDouble(2)
            (durationId,durationUserId,durationTotalTime)
          })

          if(p.deleteOld) {
            val durationDel = "delete from activity_duration_copy where day = ?"
            util.delete(durationDel,sqlDay)
          }

          duration.collect.foreach(e=>{
            val durationInsert = "insert into activity_duration_copy(day,activityId,userId,total_time) values(?,?,?,?)"
            util.insert(durationInsert,sqlDay,e._1,e._2,e._3)
          })
        */

          calendar.add(Calendar.DAY_OF_MONTH,-1)

        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }


  def fromEngToChi(str:String):String = {
    if(str != ""){
      str match {
        //活动Id
        case "6j6jbdcd4f5i" => "电视猫中秋活动"
        // 活动各页面(page)
        case "home" => "主界面"
        case "level_1" => "第一个答题页面"
        case "level_2" => "第二个答题页面"
        case "levelTimeout" => "未通关超时页面"
        case "levelFail" => "未通关答错页面"
        case "levelSuccess" => "通关扫码页面"
        case "lottery" => "H5抽奖页面浏览"
        case "lotteryFail" => "抽奖失败"
        case "lotterySuccess" => "抽奖成功"
        //用户操作行为(event)
        case "view" => "浏览"
        case "question_1" => "第一题"
        case "question_2" => "第二题"
        case "question_3" => "第三题"
        case "question_4" => "第四题"
        case "question_5" => "第五题"
        case "question_6" => "第六题"
        case "question_7" => "第七题"
        case "question_8" => "第八题"
        case "question_9" => "第九题"
        case "question_10" => "第十题"
        case "share"=>"分享"
        //未知定义
        case "undesigned" => "未知"
        case _ => "未知"
      }
    }else "未知"

  }
}
