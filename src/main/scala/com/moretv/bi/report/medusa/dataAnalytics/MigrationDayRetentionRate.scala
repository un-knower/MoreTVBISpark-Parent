package com.moretv.bi.report.medusa.dataAnalytics

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, UserIdUtils}

object MigrationDayRetentionRate extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val numOfDays = p.numOfDays
        val numOfPartition = 40

        val needToCalc = Array(1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1)
        val numOfDay = Array("d1", "d2", "d3", "d4", "d5",
                             "d6", "d7", "d8", "d9", "d10",
                             "d11", "d12", "d13", "d14", "d15",
                             "d16", "d17", "d18", "d19", "d20",
                             "d21", "d22", "d23", "d24", "d25",
                             "d26", "d27", "d28", "d29", "d30")
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
          val logUserRdd = userLog.select("userId").map(row => row.getString(0)).filter(_ != null).distinct().cache()
          Class.forName("com.mysql.jdbc.Driver")
          // 创建插入数据库连接
          val insertDB = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          val driver =insertDB.prop.getProperty("driver")
          val url = insertDB.prop.getProperty("url")
          val user = insertDB.prop.getProperty("user")
          val password = insertDB.prop.getProperty("password")
          val connection = DriverManager.getConnection(url, user, password)
          val stmt = connection.createStatement()

          for (j <- 0 until needToCalc.length) {
            c.add(Calendar.DAY_OF_MONTH, -needToCalc(j))
            val date2 = format.format(c.getTime)

            /**
              * 获取每天新增的用户信息（USERID）
              */
            val id = getID(date2, stmt)
            val min = id(0)
            val max = id(1)
            val sqlInfo = s"SELECT user_id FROM `mtv_account_migration_vice` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$date2'"
            val newUserRdd = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.MTV_ACCOUNT_MIGRATION_VICE, r => {
              (r.getString(1))}, driver, url, user, password, (min, max), numOfPartition).distinct()

            val retention = logUserRdd.intersection(newUserRdd).count()
            val newUser = newUserRdd.count().toInt
            val retentionRateAll = retention.toDouble / newUser.toDouble

            if (p.deleteOld) {
              deleteSQL(date2, stmt)
            }
            if (j == 0) {
              insertSQL(date2, "all", newUser, retentionRateAll, stmt)
            } else {
              updateSQL(numOfDay(j), "all", retentionRateAll, date2, stmt)
            }
          }
          logUserRdd.unpersist()
        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

  def getID(day: String, stmt: Statement): Array[Long] = {
    val sql = s"SELECT MIN(id),MAX(id) FROM medusa.`mtv_account_migration_vice` WHERE LEFT(openTime, 10) = '$day'"
    val id = stmt.executeQuery(sql)
    id.next()
    Array(id.getLong(1), id.getLong(2))
  }

  def insertSQL(date: String, typeInfo: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO medusa.`mtv_account_migration_retention` (day,type_info, new_num, one) VALUES('$date','$typeInfo', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, typeInfo: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.`mtv_account_migration_retention` SET $num = $retention WHERE day = '$date' and type_info ='$typeInfo'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.`mtv_account_migration_retention` WHERE day = ${date}"
    stmt.execute(sql)
  }
}