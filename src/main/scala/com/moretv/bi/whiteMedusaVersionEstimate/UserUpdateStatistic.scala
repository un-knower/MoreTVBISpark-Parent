package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by yang.qizhen on 2017/4/20.
  * 用户升级情况的统计
  * 按日新版本升级人数
  * 按日新版本累计人数 & 累计人数占比
  */
object UserUpdateStatistic extends BaseClass {
  private val tableName = "whiteMedusa_user_update_statistic"
  private val insertSql = s"insert into ${tableName}(day,total_num,proportion) values (?,?,?)"
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

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        calendar.add(Calendar.DAY_OF_MONTH,-1)
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, 0)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, date).
            registerTempTable(LogTypes.MORETV_MTV_ACCOUNT)


          sqlContext.sql(
            s"""
              |select getApkVersion(a.current_version),a.openTime,a.mac,b.version
              |from ${LogTypes.MORETV_MTV_ACCOUNT} as a
              |left join app_version_log as b
              |on getApkVersion(a.current_version) = b.version
            """.stripMargin).toDF("current_version","openTime","mac","version").registerTempTable("log")


          /** 用户升级情况的统计 **/
          val updateUserCnt = sqlContext.sql(
            s"""
               |select count(distinct case when version >= '3.1.4'
               |       and openTime <= '$insertDate 23:59:59' then mac end) as total_num,
               |       count(distinct case when version >= '3.1.4' then mac end)/count(distinct mac) as proportion
               |from log
            """.stripMargin).map(e=>(e.getLong(0),e.getDouble(1)))

          if(p.deleteOld) util.delete(deleteSql,insertDate)

          updateUserCnt.collect.foreach(e => {
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