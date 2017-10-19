package com.moretv.bi.whiteMedusaVersionEstimate

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions.udf

/**
  * Created by zhu.bingxin on 2017/5/2.
  * 按月统计登录用户数
  * 只统计新版本（3.1.4及以上的为新版本）
  * 维度：月末日期-day，月份-month
  * 度量：登录用户数-loginUser
  * 统计周期：月
  */


object MonthlyWhiteMedusaLoginUserCt extends BaseClass {

  private val table = "monthly_whiteMedusa_login_user_ct"


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
        val month = insertDay.substring(0, 7)


        //define the day
        val days = getInputPathsMonth(insertDate, 0)
        //.map(day=>DateFormatUtils.enDateAdd(day,1)) //取到所在的月的日期后，统一增加一天

        println("p.startDate is " + p.startDate)
        println("insertDay is " + insertDay)
        println("month is " + month)
        println("Input days is:")
        days.foreach(println)

        //define database
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)


        //load data
        val logLogin = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, days)
        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
          select("version").distinct().registerTempTable("app_version_log")

        logLogin.select("mac", "version")
          .registerTempTable("login_table")

        //data processings
        sqlContext.sql(
          """
            |select count(distinct x.mac) as loginUser
            |from
            |(select a.mac,b.version as version
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

        val insertSql = s"insert into $table(day,month,loginUser) " +
          "values (?,?,?)"
        if (p.deleteOld) {
          val deleteSql = s"delete from $table where day=?"
          util.delete(deleteSql, month)
        }

        resultDf.collect.foreach(e => {
          util.insert(insertSql, insertDay, month, e.get(0))
        })
        println(month + " Insert data successed!")
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
    * 取当前日期所在的月的每一天
    *
    * @param offset 当前日期月份的偏移量
    * @return 返回的是传入字符串对应日期所在的月的2号-下个月的1号（在偏移量为0的情况下）
    */
  def getInputPathsMonth(day: String, offset: Int) = {


    val format = new SimpleDateFormat("yyyyMMdd")
    val today = Calendar.getInstance()
    today.setTime(format.parse(day))
    println(today.getTime)
    today.add(Calendar.MONTH, -offset + 1)
    println(today.getTime)
    today.set(Calendar.DAY_OF_MONTH, 1)
    println(today.getTime)
    val cal = Calendar.getInstance()
    println(cal.getTime)
    cal.setTime(format.parse(day))
    println(cal.getTime)
    cal.add(Calendar.MONTH, -offset)
    println(cal.getTime)
    cal.set(Calendar.DAY_OF_MONTH, 1)
    println(cal.getTime)
    println("today.get(Calendar.DAY_OF_YEAR) is " + today.get(Calendar.DAY_OF_YEAR))
    println("cal.get(Calendar.DAY_OF_YEAR) is " + cal.get(Calendar.DAY_OF_YEAR))

    val days = today.get(Calendar.DAY_OF_YEAR) - cal.get(Calendar.DAY_OF_YEAR)
    (0 until days).map(i => {
      cal.add(Calendar.DAY_OF_YEAR, 1)
      format.format(cal.getTime)
    }).toArray

  }
}
