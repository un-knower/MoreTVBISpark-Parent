package com.moretv.bi.whiteMedusaVersionEstimate

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, UserIdUtils}

/**
  * Created by zhu.bingxin on 2017/5/4.
  * 统计新用户周留存率：统计日前7-13天新增的用户中在统计日当天到前6天活跃的用户数/统计日前7-13天新增的用户。
  * 新版本（3.1.4及以上的为新版本）
  * 依赖于WhiteMedusaUpdatedUser，该程序已经算好新增的白猫版本用户数（包括从老版本升级上来的，和直接
  */

object WeeklyWhiteMedusaNewUserRetentionRate extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate)) //输入的日期只能是周一
        cal.add(Calendar.DAY_OF_MONTH, -1) //日期变成周日，但是还是在输入周一的同一周
        val insertDay = DateFormatUtils.cnFormat.format(cal.getTime)
        val insertDate = DateFormatUtils.readFormat.format(cal.getTime)

        //define the day
        val days = DateFormatUtils.getInputPathsWeek(insertDate, 0)
          .map(day => DateFormatUtils.enDateAdd(day, 1))
        //取到所在的周的日期后，统一增加一天
        val mondayCN = DateFormatUtils.toDateCN(days(0), -1)
        val sundayCN = DateFormatUtils.toDateCN(days(6), -1)
        val weekStartEnd1 = mondayCN + "~" + sundayCN

        //val inputDate = p.startDate
        val numOfPartition = 40

        val needToCalc = Array(1, 1, 1, 1)
        val numOfDay = Array("one", "two", "three", "four")
        val format = new SimpleDateFormat("yyyy-MM-dd")
        val readFormat = new SimpleDateFormat("yyyyMMdd")
        //val date = readFormat.parse(inputDate)

        val userLog = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, days)
        val logUserID = userLog.select("mac")
          .map(row => row.getString(0)).filter(_ != null)
          .map(UserIdUtils.userId2Long).distinct().cache()
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
          cal.add(Calendar.WEEK_OF_YEAR, -needToCalc(j))
          val date2 = readFormat.format(cal.getTime)
          val days2 = DateFormatUtils.getInputPathsWeek(date2, 0)
          val mondayCN2 = DateFormatUtils.toDateCN(days2(0))
          val sundayCN2 = DateFormatUtils.toDateCN(days2(6))
          val weekStartEnd2 = mondayCN2 + "~" + sundayCN2
          val updateDay = DateFormatUtils.enDateAdd(date2, 1) //读取升级数据需要的日期


          //          val id = getID(mondayCN2,sundayCN2, stmt)
          //          val min = id(0)
          //          val max = id(1)
          //          val sqlInfo = s"SELECT mac,current_version FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) between '$mondayCN2' and '$sundayCN2'"
          //          val sqlRDD = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.MTV_ACCOUNT, r => {
          //            (r.getString(1), r.getString(2))
          //          }, driver, url, user, password, (min, max), numOfPartition)

          val updateDir = s"/log/medusa/parquet/$updateDay/white_medusa_update_user"
          val updatedUser = sqlContext.read.parquet(updateDir)
            .filter(s"date between '$mondayCN2' and '$sundayCN2'")
            .select("mac")
            .map(row => row.getString(0)).filter(_ != null)
            .map(UserIdUtils.userId2Long).distinct()
          val newUser = updatedUser.count().toInt
          val retention = logUserID.intersection(updatedUser).count()
          var retentionRate: Double = 0.0
          if (newUser != 0) {
            retentionRate = retention.toDouble / newUser.toDouble
          }
          println("this is a test###################")
          println("j is :" + j)
          println(weekStartEnd2)
          println("newUser is " + newUser)
          println("retention is " + retention)
          println("retentionRate is " + retentionRate)


          //          //全版本
          //          val sqlRDDAll = sqlRDD
          //            //.filter(_._2 < "3.1.4")
          //            .map(rdd => UserIdUtils.userId2Long(rdd._1)).distinct()
          //          val retentionAll = logUserID.intersection(sqlRDDAll).count()
          //          val newUserAll = sqlRDDAll.count().toInt
          //          val retentionRateAll = retentionAll.toDouble / newUserAll.toDouble
          //          //新版本
          //          val sqlRDDNew = sqlRDD
          //            .filter(_._2 != null)
          //            .filter(_._2.contains("_"))
          //            .map(e => (e._1, e._2.substring(e._2.lastIndexOf("_") + 1)))
          //            .filter(_._2 >= "3.1.4")
          //            .map(rdd => UserIdUtils.userId2Long(rdd._1)).distinct()
          //          val retentionNew = logUserID.intersection(sqlRDDNew).count()
          //          val newUserNew = sqlRDDNew.count().toInt
          //          val retentionRateNew = retentionNew.toDouble / newUserNew.toDouble
          //          if (p.deleteOld) {
          //            deleteSQL(weekStartEnd2, stmt1) //按照周来删除数据
          //          }
          if (j == 0) {
            insertSQL(date2, weekStartEnd2, newUser, retentionRate, stmt1)
          } else {
            updateSQL(numOfDay(j), weekStartEnd2, retentionRate, date2, stmt1)
          }
        }
        logUserID.unpersist()
      }

      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

  def getID(day1: String, day2: String, stmt: Statement): Array[Long] = {
    val sql = s"SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account` WHERE LEFT(openTime, 10) between '$day1' and '$day2'"
    val id = stmt.executeQuery(sql)
    id.next()
    Array(id.getLong(1), id.getLong(2))
  }

  def insertSQL(date: String, week_start_end: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO medusa.`weekly_white_medusa_user_retetion_day` (day,week_start_end, new_user_num, one) VALUES('$date','$week_start_end',$count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, week_start_end: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.`weekly_white_medusa_user_retetion_day` SET $num = $retention WHERE day = '$date' and week_start_end = '$week_start_end' "
    stmt.executeUpdate(sql)
  }

  def deleteSQL(week_start_end: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.`weekly_white_medusa_user_retetion_day` WHERE week_start_end = '$week_start_end'"
    stmt.execute(sql)
  }
}