package com.moretv.bi.retention

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, UserIdUtils}
import org.apache.spark.sql.functions._

object WhiteMedusaDayRetentionRate extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val numOfDays = p.numOfDays
        val numOfPartition = 40

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
          val db = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
          val driver = db.prop.getProperty("driver")
          val url = db.prop.getProperty("url")
          val user = db.prop.getProperty("user")
          val password = db.prop.getProperty("password")
          val connection = DriverManager.getConnection(url, user, password)
          val stmt = connection.createStatement()

          // 创建插入数据库连接
          val insertDB = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          val url1 = insertDB.prop.getProperty("url")
          val user1 = insertDB.prop.getProperty("user")
          val password1 = insertDB.prop.getProperty("password")
          val connection1 = DriverManager.getConnection(url1, user1, password1)
          val stmt1 = connection1.createStatement()

          for (j <- 0 until needToCalc.length) {
            c.add(Calendar.DAY_OF_MONTH, -needToCalc(j))
            val date2 = format.format(c.getTime)

            /**
              * 获取每天新增的用户信息（MAC）
              */

            //            val accountDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, date2)
            //                .withColumn("version", split(col("current_version"),"-")(4))
            //                .filter(s"left(openTime,10) = $date2")
            //
            //            val sqlInfoOld = accountDf.filter("version < '3.1.4'")
            //              .select("mac").map(_=>UserIdUtils.userId2Long(_))
            //              .distinct()
            //            val sqlInfoNew = accountDf.filter("version >= '3.1.4'")
            //              .select("mac").map(_=>UserIdUtils.userId2Long(_))
            //              .distinct()

            val id = getID(date2, stmt)
            val min = id(0)
            val max = id(1)
            val sqlInfo = s"SELECT mac,current_version FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$date2'"
            val sqlRDD = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.MTV_ACCOUNT, r => {
              (r.getString(1), r.getString(2))
            }, driver, url, user, password, (min, max), numOfPartition)
              .filter(_._2 != null)
              .filter(_._2.contains("_"))
              .map(e => (e._1, e._2.substring(e._2.lastIndexOf("_") + 1)))
            //旧版本
            val sqlRDDOld = sqlRDD
              .filter(_._2 < "3.1.4")
              .map(rdd => UserIdUtils.userId2Long(rdd._1)).distinct()
            val retentionOld = logUserID.intersection(sqlRDDOld).count()
            val newUserOld = sqlRDDOld.count().toInt
            val retentionRateOld = retentionOld.toDouble / newUserOld.toDouble
            //新版本
            val sqlRDDNew = sqlRDD
              .filter(_._2 >= "3.1.4")
              .map(rdd => UserIdUtils.userId2Long(rdd._1)).distinct()
            val retentionNew = logUserID.intersection(sqlRDDNew).count()
            val newUserNew = sqlRDDNew.count().toInt
            val retentionRateNew = retentionNew.toDouble / newUserNew.toDouble
            if (p.deleteOld) {
              deleteSQL(date2, stmt1)
            }
            if (j == 0) {
              insertSQL(date2, "old", newUserOld, retentionRateOld, stmt1)
              insertSQL(date2, "new", newUserNew, retentionRateNew, stmt1)
            } else {
              updateSQL(numOfDay(j), "old", retentionRateOld, date2, stmt1)
              updateSQL(numOfDay(j), "new", retentionRateNew, date2, stmt1)
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
    val sql = s"INSERT INTO medusa.`white_medusa_user_retetion_day` (day,version, new_user_num, one) VALUES('$date','$version', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, version: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.`white_medusa_user_retetion_day` SET $num = $retention WHERE day = '$date' and version ='$version'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.`white_medusa_user_retetion_day` WHERE day = ${date}"
    stmt.execute(sql)
  }
}