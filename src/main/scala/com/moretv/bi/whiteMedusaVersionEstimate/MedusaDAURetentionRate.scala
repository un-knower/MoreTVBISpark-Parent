package com.moretv.bi.whiteMedusaVersionEstimate

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, UserIdUtils}


/**
  * 该类统计电视猫每日活跃用户的留存情况
  */
object MedusaDAURetentionRate extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
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
          c.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE))
          val insertCal = Calendar.getInstance()
          insertCal.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE) -1)
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
            insertCal.add(Calendar.DAY_OF_MONTH,-needToCalc(j))
            val date2 = readFormat.format(c.getTime)
            val insertDate = format.format(insertCal.getTime)
            DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, date2).registerTempTable("login_log")
            val sqlRdd = sqlContext.sql(
              """
                |select distinct mac
                |from login_log
              """.stripMargin).map(r => UserIdUtils.userId2Long(r.getString(0)))
            val retention = logUserID.intersection(sqlRdd).count()
            val dauNum = sqlRdd.count().toInt
            val retentionRate = retention.toDouble / dauNum.toDouble

//            if (p.deleteOld) {
//              deleteSQL("all",insertDate, stmt1)
//            }
            if (j == 0) {
              insertSQL(insertDate, "all", dauNum, retentionRate, stmt1)
            } else {
              updateSQL(numOfDay(j), "all", retentionRate, insertDate, stmt1)
            }
          }
          logUserID.unpersist()
        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

  def insertSQL(date: String, version: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO medusa.medusa_dau_retetion_day (day,version, dau_num, one) VALUES('$date','$version', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, version: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.medusa_dau_retetion_day SET $num = $retention WHERE day = '$date' and version ='$version'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(version:String,date: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.medusa_dau_retetion_day WHERE day = '${date}' and version = '${version}'"
    stmt.execute(sql)
  }
}