package com.moretv.bi.whiteMedusaVersionEstimate

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, UserIdUtils}

/**
  * 统计314版本的老用户的留存率
  */
object WhiteMedusaOldUserRetentionRate extends BaseClass {


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
        val format = new SimpleDateFormat("yyyy-MM-dd")
        val readFormat = new SimpleDateFormat("yyyyMMdd")
        val date = readFormat.parse(inputDate)

        val calendar = Calendar.getInstance()
        calendar.setTime(date)

        for (i <- 0 until numOfDays) {
          val c = Calendar.getInstance()
          c.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE) - 1)
          val inputDate = readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH, 1)
          val userLog = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, inputDate)
          val logUserID = userLog.select("mac").map(row => row.getString(0)).filter(_ != null).
            map(UserIdUtils.userId2Long).distinct().cache()
          Class.forName("com.mysql.jdbc.Driver")

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
            val date3 = format.format(c.getTime)

            DataIO.getDataFrameOps.getDF(sc,p.paramMap,DBSNAPSHOT,LogTypes.MORETV_MTV_ACCOUNT,date2).
              registerTempTable("moretv_mtv_account")

            val olderRdd = sqlContext.sql(
              s"""
                |select distinct mac
                |from moretv_mtv_account
                |where getApkVersion(current_version) = '3.1.4' and substring(openTime,0,10) < '${date3}'
              """.stripMargin).map(e=>e.getString(0)).map(rdd => UserIdUtils.userId2Long(rdd))

            val retentionNew = logUserID.intersection(olderRdd).count()
            val newUserNew = olderRdd.count().toInt
            val retentionRateNew = retentionNew.toDouble / newUserNew.toDouble

            if (j == 0) {
              insertSQL(date2, "oldUser", newUserNew, retentionRateNew, stmt1)
            } else {
              updateSQL(numOfDay(j), "oldUser", retentionRateNew, date2, stmt1)
            }
          }
          logUserID.unpersist()
        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

  def getID(day: String, stmt: Statement): Array[Long] = {
    val sql = s"SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account` WHERE LEFT(openTime, 10) = '$day'"
    val id = stmt.executeQuery(sql)
    id.next()
    Array(id.getLong(1), id.getLong(2))
  }

  def insertSQL(date: String, version: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO medusa.`white_medusa_old_user_retetion_day` (day,version, user_num, one) VALUES('$date','$version', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, version: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.`white_medusa_old_user_retetion_day` SET $num = $retention WHERE day = '$date' and version ='$version'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.`white_medusa_old_user_retetion_day` WHERE day = ${date}"
    stmt.execute(sql)
  }
}