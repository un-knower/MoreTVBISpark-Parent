package com.moretv.bi.threshold

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by QIZHEN on 2017/5/11.
  * 计算3.1.3版本点播主要频道的平均播放时长和播放次数
  *
  */
object videoMainChannelAvgPlayStatistic extends BaseClass{
  private val tableName = "videoMainChannel_AvgPlayStatistic"
  private val insertSql = s"insert into ${tableName}(day,apkVersion,contentType,avg_playCnt,avg_playDuration) values (?,?,?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        /**连接数据库为medusa**/
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        /**开始处理日期**/
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        /**循环处理得到每一日数据并入库**/
        (0 until p.numOfDays).foreach( i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          /**点播日志**/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY, date).
            registerTempTable(LogTypes.PLAY)

           /**计算主要频道人均播放时长和人均播放次数**/
           val updatePlayCnt = sqlContext.sql(
             s"""
                |select a.apkVersion,a.contentType,
                |       a.play_cnt/a.user_cnt as avg_playCnt,
                |       b.duration/a.user_cnt as avg_playDuration
                |from
                |(select apkVersion, contentType,count(userId) as play_cnt,count(distinct userId) as user_cnt
                | from ${LogTypes.PLAY}
                | where apkVersion='3.1.3' and contentType in('movie','tv','kids','hot','comic','zongyi')
                |      and event='startplay'
                | group by apkVersion, contentType)a
                |join
                |(select apkVersion,contentType,sum(duration)/3600 as duration
                |from ${LogTypes.PLAY}
                |where apkVersion='3.1.3' and contentType in('movie','tv','kids','hot','comic','zongyi')
                |      and event<>'startplay' and duration >= 0 and duration <= 10800
                |group by apkVersion, contentType)b
                |on a.apkVersion=b.apkVersion and a.contentType=b.contentType
               """.stripMargin).map(e => (e.get(0), e.get(1), e.get(2), e.get(3)))

          if(p.deleteOld) util.delete(deleteSql,insertDate)

          updatePlayCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate,e._1, e._2,e._3,e._4)
          })

        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

}
