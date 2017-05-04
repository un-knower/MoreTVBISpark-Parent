package com.moretv.bi.whiteMedusaVersionEstimate

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, UserIdUtils}
import com.moretv.bi.whiteMedusaVersionEstimate.WhiteMedusaDAURetentionRate.{MEDUSA_DIMENSION, sc, sqlContext}


/**
  * 统计白猫版本在T+1天的回退情况
  */

object WhiteMedusaRebackStatistic extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION).
          select("version").distinct().registerTempTable("app_version_log")

        val inputDate = p.startDate
        val numOfDays = p.numOfDays

        val needToCalc = Array(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        val numOfDay = Array("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen")
        val readFormat = new SimpleDateFormat("yyyyMMdd")
        val date = readFormat.parse(inputDate)

        val calendar = Calendar.getInstance()
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_MONTH,-1)

        for (i <- 0 until numOfDays) {
          val c = Calendar.getInstance()
          c.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE) + 1)
          val inputDate = readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH, 1)
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, inputDate).registerTempTable("mtv_account")
          val logUserID = sqlContext.sql(
            """
              |select distinct mac
              |from mtv_account
              |where getApkVersion(current_version)<'3.1.4' and mac is not null
            """.stripMargin).map(e=>e.getString(0)).map(UserIdUtils.userId2Long).distinct().cache()

          // 创建插入数据库连接
          val insertDB = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          val url1 = insertDB.prop.getProperty("url")
          val user1 = insertDB.prop.getProperty("user")
          val password1 = insertDB.prop.getProperty("password")
          val connection1 = DriverManager.getConnection(url1, user1, password1)
          val stmt1 = connection1.createStatement()

          for (j <- 0 until needToCalc.length) {
            c.add(Calendar.DAY_OF_MONTH, -needToCalc(j))
            val date2 = readFormat.format(c.getTime)
            val insertCal = Calendar.getInstance()
            insertCal.setTime(DateFormatUtils.readFormat.parse(date2))
            insertCal.add(Calendar.DAY_OF_MONTH,-1)
            val insertDate = DateFormatUtils.cnFormat.format(insertCal.getTime)

            DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, date2).registerTempTable("login_user_log")

            val sqlRDDAll = sqlContext.sql(
              """
                |select a.mac,b.version
                |from login_user_log as a
                |left join app_version_log as b
                |on getApkVersion(a.version) = b.version
                |where a.mac is not null and b.version>='3.1.4'
              """.stripMargin).map(e=>UserIdUtils.userId2Long(e.getString(0))).distinct()
            val rebackRdd = logUserID.intersection(sqlRDDAll).count()
            val loginRdd = sqlRDDAll.count().toInt

            if (j == 0) {
              insertSQL(insertDate, "white", loginRdd, rebackRdd, stmt1)
            } else {
              updateSQL(numOfDay(j), "white", rebackRdd, insertDate, stmt1)
            }
          }
          logUserID.unpersist()
        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }


  def insertSQL(date: String, version: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO medusa.`white_medusa_user_reback_day` (day,version, login_user, one) VALUES('$date','$version', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, version: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.`white_medusa_user_reback_day` SET $num = $retention WHERE day = '$date' and version ='$version'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.`white_medusa_user_reback_day` WHERE day = ${date}"
    stmt.execute(sql)
  }
}