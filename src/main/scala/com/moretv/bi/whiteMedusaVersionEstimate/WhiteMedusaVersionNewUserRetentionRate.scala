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


/** *
  * Created by zhu.bingxin on 2017/9/11.
  * 电视猫分版本（3.1.3及之前、3.1.4、3.1.5、3.1.6）统计新增用户数留存（T+1~7)
  */
object WhiteMedusaVersionNewUserRetentionRate extends BaseClass {


  private val resultable = "medusa.white_medusa_version_new_user_retention"

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)

    ParamsParseUtil.parse(args) match {
      case Some(p) => {


        val tmpSqlContext = sqlContext
        import tmpSqlContext.implicits._


        val inputDate = p.startDate
        val numOfDays = p.numOfDays
        val numOfPartition = 40

        val needToCalc = Array(1, 1, 1, 1, 1, 1, 1)
        val numOfDay = Array("one", "two", "three", "four", "five", "six", "seven")
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
          val logUserID = userLog.select("mac")
            .map(row => row.getString(0)).filter(_ != null)
            //.map(UserIdUtils.userId2Long)
            .distinct().cache()
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


            val id = getID(date2, stmt)
            val min = id(0)
            val max = id(1)
            val sqlInfo = s"SELECT mac,current_version FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$date2'"
            val sqlRDD = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.MTV_ACCOUNT, r => {
              (r.getString(1), r.getString(2))
            }, driver, url, user, password, (min, max), numOfPartition)

            logUserID.toDF("mac").registerTempTable("log_user_id")
            sqlRDD.toDF("mac", "current_version").registerTempTable("mac_current_version")


            sqlContext.sql(
              """
                |select getApkVersion(a.current_version) as version,if(b.mac is null,0,1) as is_retention,a.mac
                |from mac_current_version a left join log_user_id b
                |on a.mac = b.mac
              """.stripMargin)
              .registerTempTable("version_is_retention_mac")


            sqlContext.sql(
              """
                |select a.version,a.new_user,round(b.retention_user/a.new_user,4) as retention_rate
                |from
                |(select (case WHEN version = '3.1.4' or version = '3.1.5' OR version = '3.1.6' THEN version
                |WHEN version <= '3.1.3' THEN 'old' ELSE 'other' END) as version,cast(count(distinct mac) as int) as new_user
                |from version_is_retention_mac
                |group by (case WHEN version = '3.1.4' or version = '3.1.5' OR version = '3.1.6' THEN version
                |WHEN version <= '3.1.3' THEN 'old' ELSE 'other' END)) a
                |join
                |(select (case WHEN version = '3.1.4' or version = '3.1.5' OR version = '3.1.6' THEN version
                |WHEN version <= '3.1.3' THEN 'old' ELSE 'other' END) as version,cast(count(distinct mac) as int) as retention_user
                |from version_is_retention_mac
                |where is_retention != 0
                |group by (case WHEN version = '3.1.4' or version = '3.1.5' OR version = '3.1.6' THEN version
                |WHEN version <= '3.1.3' THEN 'old' ELSE 'other' END)) b
                |on a.version = b.version
              """.stripMargin)
              .collect
              .foreach(rdd => {
                if (j == 0) {
                  insertSQL(date2, rdd.getString(0), rdd.getInt(1), rdd.getDouble(2), stmt1)
                } else {
                  updateSQL(numOfDay(j), rdd.getString(0), rdd.getDouble(2), date2, stmt1)
                }
              })

            //            //全版本
            //            val sqlRDDAll = sqlRDD
            //              //.filter(_._2 < "3.1.4")
            //              .map(rdd => UserIdUtils.userId2Long(rdd._1)).distinct()
            //            val retentionAll = logUserID.intersection(sqlRDDAll).count()
            //            val newUserAll = sqlRDDAll.count().toInt
            //            val retentionRateAll = retentionAll.toDouble / newUserAll.toDouble
            //            //新版本
            //            val sqlRDDNew = sqlRDD
            //              .filter(_._2 != null)
            //              .filter(_._2.contains("_"))
            //              .map(e => (e._1, e._2.substring(e._2.lastIndexOf("_") + 1)))
            //              .filter(_._2 >= "3.1.4")
            //              .map(rdd => UserIdUtils.userId2Long(rdd._1)).distinct()
            //            val retentionNew = logUserID.intersection(sqlRDDNew).count()
            //            val newUserNew = sqlRDDNew.count().toInt
            //            val retentionRateNew = retentionNew.toDouble / newUserNew.toDouble
            //            if (p.deleteOld) {
            //              deleteSQL(date2, stmt1)
            //            }
            //            if (j == 0) {
            //              insertSQL(date2, "all", newUserAll, retentionRateAll, stmt1)
            //              insertSQL(date2, "new", newUserNew, retentionRateNew, stmt1)
            //            } else {
            //              updateSQL(numOfDay(j), "all", retentionRateAll, date2, stmt1)
            //              updateSQL(numOfDay(j), "new", retentionRateNew, date2, stmt1)
            //            }
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
    val sql = s"INSERT INTO $resultable (day,version, new_user_num, one) VALUES('$date','$version', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, version: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE $resultable SET $num = $retention WHERE day = '$date' and version ='$version'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date: String, stmt: Statement) = {
    val sql = s"DELETE FROM $resultable WHERE day = ${date}"
    stmt.execute(sql)
  }
}