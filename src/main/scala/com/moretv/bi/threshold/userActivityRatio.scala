package com.moretv.bi.threshold

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by QIZHEN on 2017/5/12.
  * 计算3.1.3版本用户活跃度=登录人数/总人数
  */
object userActivityRatio extends BaseClass{
  private val tableName = "userActivityRatio"
  private val insertSql = s"insert into ${tableName}(day,apkVersion,userActivityRatio) values (?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"


  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        /**连接数据库**/
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
          val accountDate= DateFormatUtils.readFormat.format(calendar.getTime)

          /**启动日志**/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, date)
            .registerTempTable(LogTypes.LOGINLOG)

          /**计算新增和累计日志**/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, accountDate).
            registerTempTable(LogTypes.MORETV_MTV_ACCOUNT)


          sqlContext.sql(
            s"""
               |select case when version like '%3.1.3%' then '3.1.3' end  as version,mac
               |from ${LogTypes.LOGINLOG}
              """.stripMargin).toDF("version","mac").registerTempTable("login_log")

          sqlContext.sql(
            s"""
               |select case when current_version like '%3.1.3%' then '3.1.3' end  as current_version,mac,openTime
               |from ${LogTypes.MORETV_MTV_ACCOUNT}
              """.stripMargin).toDF("current_version","mac","openTime").registerTempTable("account_log")

          /**计算用户活跃率**/
          val updatePlayCnt = sqlContext.sql(
            s"""
               |select a.version,a.loginUser_cnt/c.totalUser_cnt as userActivityRatio
               |from
               |(select version,count(distinct mac) as loginUser_cnt
               |from login_log
               |where version='3.1.3'
               |group by version)a
               |join
               |(select current_version,count(distinct mac) as totalUser_cnt
               | from account_log
               |where current_version = '3.1.3'
               |      and  openTime < '$insertDate 23:59:59'
               |group by current_version)c
               |on a.version=c.current_version
               """.stripMargin).map(e => (e.get(0), e.get(1)))


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
