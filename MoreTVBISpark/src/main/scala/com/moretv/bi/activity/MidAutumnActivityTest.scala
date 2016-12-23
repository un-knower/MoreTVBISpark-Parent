package com.moretv.bi.activity

import java.util.Calendar

import org.apache.spark.sql.functions._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by czw on 16/9/12.
  * 专用于统计活动的相关数据,每次需修改活动的上线时间onLineDay
  */
object MidAutumnActivityTest extends BaseClass {


  private val pageViewSingleDayInsert =
    "insert into midautumn_activity_pageview_singleday(day,activityId,page,source,access_num,user_num) values(?,?,?,?,?,?)"

  private val pageViewSetDayInsert =
    "insert into midautumn_activity_pageview_dataset (day,activityId,page,source,access_num,user_num) values(?,?,?,?,?,?)"

  private val operationSingleDayInsert =
    "insert into midautumn_activity_operation_singleday (day,activityId,page,source,event,access_num,user_num) values(?,?,?,?,?,?,?)"

  private val operationSetDayInsert =
    "insert into midautumn_activity_operation_dataset (day,activityId,page,source,event,access_num,user_num) values(?,?,?,?,?,?,?)"

  def main(args: Array[String]) {
    ModuleClass.executor(MidAutumnActivityTest, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        val calendar = Calendar.getInstance()
        val startDay = p.startDate
        calendar.setTime(DateFormatUtils.readFormat.parse(startDay))

        //活动上线时间,用于构造日期集合,保证其时间比当天早
        val onLineDay = "20160916"

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


          //单天
          if (p.deleteOld) {
            val pageViewSingleDayDel = "delete from midautumn_activity_pageview_singleday where day = ?"
            util.delete(pageViewSingleDayDel, sqlDay)
          }
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, ACTIVITY, LogTypes.PAGEVIEW, logDay)
            .filter("event = 'view'")
            .groupBy("activityId", "page", "source")
            .agg(count("userId"), countDistinct("userId"))
            .collect.foreach(e => {
            util.insert(pageViewSingleDayInsert, sqlDay,
              fromEngToChi(e.getString(0)), fromEngToChi(e.getString(1)), fromEngToChi(e.getString(2)),
              e.getLong(3), e.getLong(4)
            )
          })

          //一定天数
          if (p.deleteOld) {
            val pageViewSetDayDel = "delete from midautumn_activity_pageview_dataset where day = ?"
            util.delete(pageViewSetDayDel, sqlDay)
          }

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, ACTIVITY, LogTypes.PAGEVIEW, logDaySet)
            .filter("event = 'view'")
            .groupBy("activityId", "page", "source")
            .agg(count("userId"), countDistinct("userId"))
            .collect.foreach(e => {
            util.insert(pageViewSetDayInsert, sqlDay,
              fromEngToChi(e.getString(0)), fromEngToChi(e.getString(1)), fromEngToChi(e.getString(2)),
              e.getLong(3), e.getLong(4))
          })

          //各页面的其他行为人数/次数(区分页面来源)
          if (p.deleteOld) {
            val operationSingleDayDel = "delete from midautumn_activity_operation_singleday where day = ?"
            util.delete(operationSingleDayDel, sqlDay)
          }

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, ACTIVITY, LogTypes.OPERATION, logDay)
            .filter("event != 'view'")
            .groupBy("activityId", "page", "source", "event")
            .agg(count("userId"), countDistinct("userId"))
            .collect.foreach(e => {
            util.insert(operationSingleDayInsert, sqlDay,
              fromEngToChi(e.getString(0)), fromEngToChi(e.getString(1)), fromEngToChi(e.getString(2)), fromEngToChi(e.getString(3)),
              e.getLong(4), e.getLong(5)
            )
          })

          //各页面的其他行为总人数/次数(区分页面来源)
          if (p.deleteOld) {
            val operationSetDayDel = "delete from midautumn_activity_operation_dataset where day = ?"
            util.delete(operationSetDayDel, sqlDay)
          }
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, ACTIVITY, LogTypes.OPERATION, logDaySet)
            .filter("event ! = 'view'")
            .groupBy("activityId", "page", "source", "event")
            .agg(count("userId"), countDistinct("userId"))
            .collect.foreach(e => {

            util.insert(operationSetDayInsert, sqlDay,
              fromEngToChi(e.getString(0)), fromEngToChi(e.getString(1)), fromEngToChi(e.getString(2)), fromEngToChi(e.getString(3)),
              e.getLong(4), e.getLong(5))
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
        case "share" => "分享"
        //未知定义
        case "undesigned" => "未知"
        case _ => "未知"
      }
    } else "未知"

  }
}
