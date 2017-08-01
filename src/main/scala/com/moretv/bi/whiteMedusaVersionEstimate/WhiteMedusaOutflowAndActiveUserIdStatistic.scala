package com.moretv.bi.whiteMedusaVersionEstimate

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, UserIdUtils}


/**
  * 统计白猫版本的用户在T+5内均活跃以及未活跃的用户在3月份的播放情况对比
  */

object WhiteMedusaOutflowAndActiveUserIdStatistic extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
          select("version").distinct().registerTempTable("app_version_log")

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        for (i <- 0 until p.numOfDays) {
          val pathDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val accountDate = DateFormatUtils.readFormat.format(cal.getTime)

          val outDir = s"/log/medusa/temple/userId/${pathDate}"

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,LOGINLOG,LogTypes.LOGINLOG,pathDate).select("mac","version").
            repartition(100).
            registerTempTable("login_log")

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,DBSNAPSHOT,LogTypes.MORETV_MTV_ACCOUNT,accountDate).
            select("mac","user_id").repartition(100).registerTempTable("account_log")

          val calendar = Calendar.getInstance()
          calendar.setTime(DateFormatUtils.readFormat.parse(pathDate))
          val dateArr = new scala.collection.mutable.ArrayBuffer[String]()
          (0 until 5).foreach(j=>{
            calendar.add(Calendar.DAY_OF_MONTH,1)
            dateArr.+=(DateFormatUtils.readFormat.format(calendar.getTime))
          })
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,LOGINLOG,LogTypes.LOGINLOG,dateArr.toArray).select("mac","date").
            repartition(100).registerTempTable("checkout_login_log")


          /**
            * 统计T+5未登录的用户
            */
          sqlContext.sql(
            """
              |select a.mac,b.mac
              |from login_log as a
              |left join checkout_login_log as b
              |on a.mac = b.mac
              |where getApkVersion(a.version) = '3.1.4'
            """.stripMargin).toDF("prefer_mac","login_mac").repartition(100).registerTempTable("mac_log")


          sqlContext.sql(
            """
              |select b.user_id
              |from mac_log as a
              |join account_log as b
              |on a.prefer_mac = b.mac
              |where login_mac is null
            """.stripMargin).toDF("userId").repartition(100).registerTempTable("outflow_log")


          /**
            * 统计T+5每天登录的用户
            */
          sqlContext.sql(
            """
              |select mac,count(distinct date) as num
              |from checkout_login_log
              |group by mac
              |having num>=3
            """.stripMargin).toDF("mac","num").repartition(100).registerTempTable("active_log")

          sqlContext.sql(
            """
              |select a.mac
              |from login_log as a
              |join active_log as b
              |on a.mac = b.mac
            """.stripMargin).repartition(100).registerTempTable("mac_log")

          sqlContext.sql(
            """
              |select a.user_id
              |from account_log as a
              |join mac_log as b
              |on a.mac = b.mac
            """.stripMargin).toDF("userId").repartition(100).registerTempTable("active_userId_log")

          /**
            * 合并userId
            */
          sqlContext.sql(
            """
              |select 'outflow' as userType, userId
              |from outflow_log
              |union
              |select 'active' as userType, userId
              |from active_userId_log
            """.stripMargin).toDF("userType","userId").repartition(50).write.parquet(outDir)





        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

}