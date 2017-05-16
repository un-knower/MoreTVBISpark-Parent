package com.moretv.bi.threshold

import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.login.DailyActiveUserByMac.{LOGINLOG, sc, sqlContext}
import com.moretv.bi.threshold.videoMainChannelAvgPlayStatistic._
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.whiteMedusaVersionEstimate.classifyContentTypePlayRatio.sqlContext

/**
  * Created by whaley on 2017/5/11.
  * 计算3.1.3版本点播播放人数比=点播播放人数/日活人数
  */
object videoPlayUserRatio extends BaseClass{
  private val tableName = "videoPlayUserRatio"
  private val insertSql = s"insert into ${tableName}(day,apkVersion,playUserRatio) values (?,?,?)"
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

          /**启动日志**/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, date)
            .registerTempTable(LogTypes.LOGINLOG)

          sqlContext.sql(
            s"""
                |select case when version like '%3.1.3%' then '3.1.3' end  as version,mac
                |from ${LogTypes.LOGINLOG}
              """.stripMargin).toDF("version","mac").registerTempTable("login_log")


          /**计算播放人数比**/
          val updatePlayCnt = sqlContext.sql(
            s"""
                |select a.apkVersion,playUser_cnt/activeUser_cnt as playUserRatio
                |from
                |(select apkVersion,count(distinct userId) as playUser_cnt
                |from ${LogTypes.PLAY}
                |where apkVersion='3.1.3' and event='startplay'
                |group by apkVersion)a
                |join
                |(select version,count(distinct mac) as activeUser_cnt
                |from login_log
                |where version = '3.1.3'
                |group by version)b
                |on a.apkVersion=b.version
               """.stripMargin).map(e => (e.get(0), e.get(1)))

          if(p.deleteOld) util.delete(deleteSql,insertDate)

          updatePlayCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate,e._1, e._2)
          })

        })
      }

      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}