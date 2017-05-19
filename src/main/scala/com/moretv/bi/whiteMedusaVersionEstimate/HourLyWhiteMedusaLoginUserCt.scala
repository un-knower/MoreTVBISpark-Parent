package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions.{col, udf}

/**
  * Created by zhu.bingxin on 2017/5/4.
  * 统计登录用户数
  * 新版本（3.1.4及以上的为新版本）
  * 每天按小时统计
  */
object HourLyWhiteMedusaLoginUserCt extends BaseClass {

  private val table = "hourly_white_medusa_login_active_user_ct"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  /**
    * this method do not complete.Sub class that extends BaseClass complete this method
    */
  override def execute(args: Array[String]): Unit = {


    /**
      * UDF
      */
    val udfToDateCN = udf { yyyyMMdd: String => DateFormatUtils.toDateCN(yyyyMMdd) }
    sqlContext.udf.register("getApkVersion", getApkVersion _)
    sqlContext.udf.register("getVersion", getVersion _)

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)
        (0 until p.numOfDays).foreach(i => {

          //define the day
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date)

          //define database
          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

          //add 1 day after read currrent day
          cal.add(Calendar.DAY_OF_MONTH, 1)
          val date2 = DateFormatUtils.readFormat.format(cal.getTime)

          //load data
          //val logAccount = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, date)
          val logLogin = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, date2)
          DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
            select("version").distinct().registerTempTable("app_version_log")

          //filter data
          logLogin.select("datetime", "mac", "version")
            .registerTempTable("login_table")

          //data processings

          sqlContext.sql(
            """
              |select x.hour,count(distinct x.mac) as loginUser
              |from
              |(select substring(a.datetime,12,2) as hour,a.mac,getVersion(b.version) as version
              |from login_table a
              |left join app_version_log b
              |on getApkVersion(a.version) = b.version) x
              |where x.version = 'new'
              |group by hour
            """.stripMargin)
            .registerTempTable("login_users")

          val resultDf = sqlContext.sql(
            """
              |select *
              |from login_users
            """.stripMargin)

          val insertSql = s"insert into $table(day,hour,loginUser) " +
            "values (?,?,?)"
          if (p.deleteOld) {
            val deleteSql = s"delete from $table where day=?"
            util.delete(deleteSql, insertDate)
          }

          resultDf.collect.foreach(e => {
            util.insert(insertSql, insertDate, e.get(0), e.get(1))
          })
          println(insertDate + " Insert data successed!")
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }


  /**
    * 从apkSerial中提取出apkVersion
    *
    * @param apkSerials
    * @return
    */
  def getApkVersion(apkSerials: String) = {
    if (apkSerials != null) {
      if (apkSerials == "")
        "kong"
      else if (apkSerials.contains("_")) {
        apkSerials.substring(apkSerials.lastIndexOf("_") + 1)
      } else {
        apkSerials
      }
    } else
      "null"
  }

  /**
    * 将version新旧版区分开
    */
  def getVersion(apkVersion: String) = {
    if (apkVersion != null && apkVersion >= "3.1.4") "new"
    else "old"
  }
}
