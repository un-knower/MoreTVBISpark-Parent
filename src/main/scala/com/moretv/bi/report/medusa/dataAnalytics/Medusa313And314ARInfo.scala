package com.moretv.bi.report.medusa.dataAnalytics

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.whiteMedusaVersionEstimate.ApkVersionUtil

/**
  * Created by Chubby on 2017/5/8.
  * 该类用于统计3.1.3&314版本的日活率
  */
object Medusa313And314ARInfo extends BaseClass{
  private val insertSql = "insert into tmp_medusa_ar_info(day,version,dau,total_user) values(?,?,?,?)"
  private val deleteSql = "delete from tmp_medusa_ar_info where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) = {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)
    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.startDate))


        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
          select("version").distinct().registerTempTable("app_version_log")

        (0 until p.numOfDays).foreach(i=>{
          val loadDate = DateFormatUtils.readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val insertDate = DateFormatUtils.cnFormat.format(calendar.getTime)
          val dbsnapshotLoadDate = DateFormatUtils.readFormat.format(calendar.getTime)

          /**
            * 加载登录信息
            */
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,LOGINLOG,LogTypes.LOGINLOG,loadDate).
            registerTempTable("login_log")

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,DBSNAPSHOT,LogTypes.MORETV_MTV_ACCOUNT,dbsnapshotLoadDate).
            registerTempTable("account_log")

          if(p.startDate < "2017-04-26"){

            val activeUser = sqlContext.sql(
              s"""
                 |select distinct a.mac
                 |from login_log as a
                 |join account_log as b
                 |on a.mac = b.mac
                 |where a.mac is not null and a.version like '%3.1.3' and substring(b.openTime,0,10) < '${insertDate}'
            """.stripMargin).count()

            val totalUser = sqlContext.sql(
              s"""
                 |select distinct mac
                 |from account_log
                 |where mac is not null and current_version like '%3.1.3' and substring(openTime,0,10) <= '${insertDate}'
            """.stripMargin).count()

            if(p.deleteOld) util.delete(deleteSql,insertDate)

            util.insert(insertSql,insertDate,"313",activeUser,totalUser)
          }else{

            val activeUser313 = sqlContext.sql(
              s"""
                 |select distinct a.mac
                 |from login_log as a
                 |join account_log as b
                 |on a.mac = b.mac
                 |where a.mac is not null and a.version like '%3.1.3' and substring(b.openTime,0,10) < '${insertDate}'
            """.stripMargin).count()

            val totalUser313 = sqlContext.sql(
              s"""
                 |select distinct mac
                 |from account_log
                 |where mac is not null and current_version like '%3.1.3' and substring(openTime,0,10) <= '${insertDate}'
            """.stripMargin).count()


            val activeUser314 = sqlContext.sql(
              s"""
                 |select distinct a.mac
                 |from login_log as a
                 |join account_log as b
                 |on a.mac = b.mac
                 |where a.mac is not null and a.version like '%3.1.4' and substring(b.openTime,0,10) < '${insertDate}'
            """.stripMargin).count()

            val totalUser314 = sqlContext.sql(
              s"""
                 |select distinct mac
                 |from account_log
                 |where mac is not null and current_version like  '%3.1.4' and substring(openTime,0,10) <= '${insertDate}'
            """.stripMargin).count()

            if(p.deleteOld) util.delete(deleteSql,insertDate)

            util.insert(insertSql,insertDate,"313",activeUser313,totalUser313)

            util.insert(insertSql,insertDate,"314",activeUser314,totalUser314)
          }


        })

      }
      case None => {}
    }
  }
}
