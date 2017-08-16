package com.moretv.bi.live

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by QIZHEN on 2017/8/14.
  * 全网直播站点树的播放人数/次数/时长
  */
object WebcastSiteTreePlayInformation extends BaseClass{
  /**定义存储全网直播站点树的播放人数/次数/时长的表**/
  private val tableName = "live_webcast_sitetree_play_information"
  private val insertSql = s"insert into ${tableName}(date,site_tree,play_num,play_user,play_duration,play_duration_user) values (?,?,?,?,?,?)"
  private val deleteSql = s"delete from ${tableName} where date = ?"

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

          /**统计播放人数、次数、时长**/
          val updateUserCnt = sqlContext.sql(
            s"""
               |select a.*,b.play_duration,b.play_duration_user
               |from
               |(select substring(pathMain,27) as sitetree,
               |       count(userId) as play_num,
               |       count(distinct userId) as play_user
               |from ${LogTypes.LIVE}
               |where event='startplay' and liveType='live' and sourceType='webcast'
               |      and pathMain like '%webcast-webcast%'
               |group by substring(pathMain,27))a
               |join
               |(select substring(pathMain,27) as sitetree,
               |       sum(duration) as play_duration,
               |       count(distinct userId) as play_duration_user
               |from ${LogTypes.LIVE}
               |where event<>'startplay' and liveType='live' and sourceType='webcast'
               |      and duration between 0 and 36000
               |      and pathMain like '%webcast-webcast%'
               |group by substring(pathMain,27))b
               |on a.sitetree = b.sitetree
               """.stripMargin).map(e => (e.get(0), e.get(1), e.get(2) , e.get(3) , e.get(4)))

        //  if (p.deleteOld) util.delete(deleteSql, insertDate)

          updateUserCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate, e._1, e._2, e._3, e._4, e._5)
          })


        })

      }
    }

  }

}
