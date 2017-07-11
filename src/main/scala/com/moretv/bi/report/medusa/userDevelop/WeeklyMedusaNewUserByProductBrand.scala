package com.moretv.bi.report.medusa.userDevelop

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions.udf

/**
  * Created by zhu.bingxin on 2017/5/2.
  * 按周统计登录用户数
  * 只统计新版本（3.1.4及以上的为新版本）
  * 维度：周日日期-day，开始和结束日期-weekStartEnd
  * 度量：登录用户数-loginUser
  * 统计周期：周
  */


object WeeklyMedusaNewUserByProductBrand extends BaseClass {

  private val table = "weekly_whiteMedusa_login_user_ct"


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
        val insertDay = DateFormatUtils.cnFormat.format(cal.getTime)
        val insertDate = DateFormatUtils.readFormat.format(cal.getTime)

        //define the day
        val days = getInputPathsWeek(insertDate, 0)
          .map(day => DateFormatUtils.enDateAdd(day, 1))
        //取到所在的周的日期后，统一增加一天
        val mondayCN = DateFormatUtils.toDateCN(days(0), -1)
        val sundayCN = DateFormatUtils.toDateCN(days(6), -1)
        val weekStartEnd = mondayCN + "~" + sundayCN
        //val date = DateFormatUtils.readFormat.format(cal.getTime)
        //val insertDate = DateFormatUtils.toDateCN(date)
        //        println("p.startDate is " + p.startDate)
        //        println("mondayCN is " + mondayCN)
        //        println("sundayCN is " + sundayCN)
        //        println("weekStartEnd is " + weekStartEnd)
        //        println("Input days is:")
        //        days.foreach(println)

        //define database
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        //load data
        val logLogin = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, days)
        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
          select("version").distinct().registerTempTable("app_version_log")

        logLogin.select("day", "mac", "version")
          .registerTempTable("login_table")

        //data processings
        sqlContext.sql(
          """
            |select count(distinct x.mac) as loginUser
            |from
            |(select a.day,a.mac,b.version as version
            |from login_table a
            |left join app_version_log b
            |on getApkVersion(a.version) = b.version) x
            |where x.version >= '3.1.4'
          """.stripMargin)
          .registerTempTable("login_users")

        val resultDf = sqlContext.sql(
          s"""
             |select loginUser
             |from login_users
          """.stripMargin)

        val insertSql = s"insert into $table(day,weekStartEnd,loginUser) " +
          "values (?,?,?)"
        if (p.deleteOld) {
          val deleteSql = s"delete from $table where day=?"
          util.delete(deleteSql, insertDay)
        }

        resultDf.collect.foreach(e => {
          util.insert(insertSql, insertDay, weekStartEnd, e.get(0))
        })
        println(insertDay + " Insert data successed!")
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


  /**
    * 取当前日期所在的周的每一天
    *
    * @param offset 当前日期的偏移量
    * @return 返回的是传入日期day所在自然周的日期，自然周是指周一到周日
    */
  def getInputPathsWeek(day: String, offset: Int) = {

    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(day))
    cal.add(Calendar.WEEK_OF_MONTH, -1)
    cal.add(Calendar.WEEK_OF_YEAR, offset)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    val monday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY)
    val tuesday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.WEDNESDAY)
    val wednesday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY)
    val thursday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY)
    val friday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY)
    val saturday = format.format(cal.getTime)
    cal.add(Calendar.WEEK_OF_YEAR, 1)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
    val sunday = format.format(cal.getTime)

    Array(monday, tuesday, wednesday, thursday, friday, saturday, sunday)

  }
}
