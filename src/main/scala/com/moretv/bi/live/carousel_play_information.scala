package com.moretv.bi.live

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by QIZHEN on 2017/5/5.
  */
object carousel_play_information extends BaseClass {
  /**定义存储轮播播放次数、人数和播放时长数据的表**/
  private val tableName = "live_carousel_play_information"
  private val insertSql = s"insert into ${tableName}(day,sourceType,user_num,play_num,avg_play,avg_duration) values (?,?,?,?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"

  /**定义存储按播放人数统计轮播频道收视TOP200节目的表**/
  private val tableName1 = "live_carousel_user_top200"
  private val insertSql1 = s"insert into ${tableName1}(day,liveName,channelSid,user_num) values (?,?,?,?)"
  private val deleteSql1 = s"delete from ${tableName1} where day = ?"

  /**定义存储按播放次数统计轮播频道收视TOP200节目的表**/
  private val tableName2 = "live_carousel_play_top200"
  private val insertSql2 = s"insert into ${tableName2}(day,liveName,channelSid,play_num) values (?,?,?,?)"
  private val deleteSql2 = s"delete from ${tableName2} where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        /** 连接数据库为medusa **/
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        /** 开始处理日期 **/
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        /** 循环处理得到每一日数据并入库 **/
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          /** 直播日志 **/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.LIVE, date).
            registerTempTable(LogTypes.LIVE)

          /**计算轮播的播放人数、播放次数和人均播放时长**/
          val updateCnt = sqlContext.sql(
            s"""
               |select a.sourceType,a.user_num,a.play_num, a.play_num/a.user_num as avg_play, b.total_duration/b.user_duration as avg_duration
               |from
               |(select sourceType,
               |        count(distinct userId) as user_num,
               |        count(userId) as play_num
               |from ${LogTypes.LIVE}
               |where event='startplay' and liveType='live' and sourceType='carousel'
               |group by sourceType)a
               |join
               |(select sourceType,count(distinct userId) as user_duration
               |        sum(duration)/60 as total_duration
               |from ${LogTypes.LIVE}
               |where event='switchchannel' and liveType='live' and sourceType='carousel'
               |      and duration >=0 and duration <=36000
               |group by sourceType)b
               |on a.sourceType = b.sourceType
            """.stripMargin).map(e => (e.get(0), e.get(1), e.get(2), e.get(3), e.get(4)))

          if (p.deleteOld) util.delete(deleteSql, insertDate)

          updateCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate, e._1, e._2, e._3, e._4,e._5)
          })

          /**按播放人数统计轮播频道收视TOP200节目**/
          val updateUserCnt = sqlContext.sql(
            s"""
               |select liveName,channelSid,
               |       count(distinct userId) as user_num
               |from ${LogTypes.LIVE}
               |where event='startplay' and liveType='live' and sourceType='carousel'
               |group by liveName,channelSid
               |order by user_num desc
               |limit 200
               """.stripMargin).map(e => (e.get(0), e.get(1), e.get(2)))

          if (p.deleteOld) util.delete(deleteSql1, insertDate)

          updateUserCnt.collect.foreach(e => {
            util.insert(insertSql1, insertDate, e._1, e._2, e._3)
          })

          /**按播放次数统计轮播频道收视TOP200节目**/
          val updatePlayCnt = sqlContext.sql(
            s"""
               |select liveName,channelSid,
               |       count(userId) as play_num
               |from ${LogTypes.LIVE}
               |where event='startplay' and liveType='live' and sourceType='carousel'
               |group by liveName,channelSid
               |order by play_num desc
               |limit 200
               """.stripMargin).map(e => (e.get(0), e.get(1), e.get(2)))

          if (p.deleteOld) util.delete(deleteSql2, insertDate)

          updatePlayCnt.collect.foreach(e => {
            util.insert(insertSql2, insertDate, e._1, e._2, e._3)
          })

        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
