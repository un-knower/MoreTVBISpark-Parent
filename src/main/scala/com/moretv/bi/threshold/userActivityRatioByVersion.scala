package com.moretv.bi.threshold

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.whiteMedusaVersionEstimate.ApkVersionUtil

/**
  * Created by QIZHEN on 2017/5/26.
  * 计算所有版本用户活跃度=登录人数/总人数
  */
object userActivityRatioByVersion extends BaseClass{
  private val tableName = "userActivityRatioByVersion"
  private val insertSql = s"insert into ${tableName}(day,apkVersion,loginUser_cnt,totalUser_cnt,userActivityRatio) values (?,?,?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"


  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
          select("version").distinct().registerTempTable("app_version_log")

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
               |select getApkVersion(a.current_version),a.openTime,a.mac,b.version
               |from ${LogTypes.MORETV_MTV_ACCOUNT} as a
               |left join app_version_log as b
               |on getApkVersion(a.current_version) = b.version
            """.stripMargin).toDF("current_version","openTime","mac","version").registerTempTable("account_log")

          sqlContext.sql(
            s"""
               |select getApkVersion(a.version),a.mac,b.version
               |from ${LogTypes.LOGINLOG} as a
               |left join app_version_log as b
               |on getApkVersion(a.version) = b.version
            """.stripMargin).toDF("current_version","mac","version").registerTempTable("login_log")


          /**计算用户活跃率**/
          val updatePlayCnt = sqlContext.sql(
            s"""
               |select a.current_version,a.loginUser_cnt,c.totalUser_cnt,a.loginUser_cnt/c.totalUser_cnt as userActivityRatio
               |from
               |(select current_version,count(distinct mac) as loginUser_cnt
               |from login_log
               |group by current_version)a
               |join
               |(select current_version,count(distinct mac) as totalUser_cnt
               | from account_log
               |where openTime <='$insertDate 23:59:59'
               |group by current_version)c
               |on a.current_version=c.current_version
               """.stripMargin).map(e => (e.get(0), e.get(1),e.get(2),e.get(3)))


          updatePlayCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate,e._1, e._2, e._3, e._4)
          })

        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
