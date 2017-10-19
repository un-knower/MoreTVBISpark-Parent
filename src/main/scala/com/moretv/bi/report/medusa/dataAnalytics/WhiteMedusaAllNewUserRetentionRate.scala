package com.moretv.bi.report.medusa.dataAnalytics

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, UserIdUtils}
import com.moretv.bi.whiteMedusaVersionEstimate.ApkVersionUtil


/**
  * 该类统计白猫版本每日升级用户的留存情况
  */
object WhiteMedusaAllNewUserRetentionRate extends BaseClass {


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
//
//        val needToCalc = Array(
//          1,1,1,1,1,1,1,1,1,1,
//          1,1,1,1,1,1,1,1,1,1,
//          1,1,1,1,1,1,1,1,1,1,
//          1,1,1,1,1,1,1,1,1,1,
//          1,1,1,1,1,1,1,1,1,1,
//          1,1,1,1,1,1,1,1,1,1)
        val needToCalc = Array(1,1,1,1)
        val numOfDay = Array(
          "d1","d2","d3","d4","d5","d6","d7","d8","d9","d10",
          "d11","d12","d13","d14","d15","d16","d17","d18","d19","d20",
          "d21","d22","d23","d24","d25","d26","d27","d28","d29","d30",
          "d31","d32","d33","d34","d35","d36","d37","d38","d39","d40",
          "d41","d42","d43","d44","d45","d46","d47","d48","d49","d50",
          "d51","d52","d53","d54","d55","d56","d57","d58","d59","d60")
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
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, inputDate).
            registerTempTable("today_login_log")

          val logUserID = sqlContext.sql(
            """
              |select a.mac,b.version
              |from today_login_log as a
              |left join app_version_log as b
              |on getApkVersion(a.version) = b.version
              |where a.mac is not null and b.version>='3.1.4'
            """.stripMargin).map(e=>UserIdUtils.userId2Long(e.getString(0))).distinct().cache()

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
            DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.WHITE_MEDUSA_UPDATE_USER, date2).registerTempTable("update_log")

            val sqlRdd = sqlContext.sql(
              s"""
                |select mac
                |from update_log
                |where mac is not null and date = '${insertDate}'
              """.stripMargin).map(e=>UserIdUtils.userId2Long(e.getString(0))).distinct()
            val retention = logUserID.intersection(sqlRdd).count()
            val update_num = sqlRdd.count().toInt

            println(s"========The inputdate is ${inputDate}")

            println(s"=======The update number is: ${update_num}")
            println(s"========The date is ${insertDate}")
            println(s"=========The date is ${date2}")
            var retentionRate:Double = 0.0
            if(update_num != 0){
              retentionRate = retention.toDouble / update_num.toDouble
            }

            if (p.deleteOld) {
              deleteSQL("white",insertDate, stmt1)
            }
            if (j == 0) {
              insertSQL(insertDate, "white", update_num, retentionRate, stmt1)
            } else {
              updateSQL(numOfDay(j), "white", retentionRate, insertDate, stmt1)
            }
          }
          logUserID.unpersist()
        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

  def insertSQL(date: String, version: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO medusa.white_update_user_retention_day_for_model (day,version, update_num, d1) VALUES('$date','$version', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, version: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.white_update_user_retention_day_for_model SET $num = $retention WHERE day = '$date' and version ='$version'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(version:String,date: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.white_update_user_retention_day_for_model WHERE day = '${date}' and version = '${version}'"
    stmt.execute(sql)
  }
}