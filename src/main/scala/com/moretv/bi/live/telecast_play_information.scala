package com.moretv.bi.live

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.live.webcast_play_information._
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by QIZHEN on 2017/5/8.
  */
object telecast_play_information extends BaseClass{
  /**定义存储按播放人数统计频道直播收视TOP200节目的表**/
  private val tableName = "live_telecast_user_top200"
  private val insertSql = s"insert into ${tableName}(day,liveName,user_num) values (?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"

  /**定义存储按播放人数统计频道直播收视TOP200节目的表**/
  private val tableName1 = "live_telecast_play_top200"
  private val insertSql1 = s"insert into ${tableName1}(day,liveName,play_num) values (?,?,?)"
  private val deleteSql1 = s"delete from ${tableName1} where day = ?"

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

          /**按播放人数统计全网直播收视TOP200节目**/
          val updateUserCnt = sqlContext.sql(
            s"""
               |select liveName,
               |       count(distinct userId) as user_num
               |from ${LogTypes.LIVE}
               |where event='startplay' and liveType='live' and sourceType='telecast'
               |group by liveName
               |order by user_num desc
               |limit 200
               """.stripMargin).map(e => (e.get(0), e.get(1)))

          if (p.deleteOld) util.delete(deleteSql, insertDate)

          updateUserCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate, e._1, e._2)
          })


          /**按播放次数统计全网直播收视TOP200节目**/
          val updatePlayCnt = sqlContext.sql(
            s"""
               |select liveName,
               |       count(userId) as play_num
               |from ${LogTypes.LIVE}
               |where event='startplay' and liveType='live' and sourceType='telecast'
               |group by liveName
               |order by play_num desc
               |limit 200
               """.stripMargin).map(e => (e.get(0), e.get(1)))

          if (p.deleteOld) util.delete(deleteSql1, insertDate)

          updatePlayCnt.collect.foreach(e => {
            util.insert(insertSql1, insertDate, e._1, e._2)
          })
        })



      }
    }
  }
}
