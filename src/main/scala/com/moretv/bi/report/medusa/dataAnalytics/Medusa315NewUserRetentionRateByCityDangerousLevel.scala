package com.moretv.bi.report.medusa.dataAnalytics

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, UserIdUtils}
import com.moretv.bi.whiteMedusaVersionEstimate.ApkVersionUtil
import org.apache.spark.sql.SQLContext


/**
  * 临时需求
  * 【部门】软件产品部
  * 【业务线】电视猫
  * 【希望数据提供时间】
  * 【需求目的】根据3.1.5版本数据，推算3.1.6版本T+N(1,2,3,7,14,20,30)留存
  * 【需要数据的时间区间】6月25日 -7月25日 每日
  * 【需求具体内容说明及相关附件】需要每天，拆分高危/低危/中危地区；
  */
object Medusa315NewUserRetentionRateByCityDangerousLevel extends BaseClass {

  private val resultTable = "medusa.medusa_315_new_user_retention_rate_by_city_dangerous_level"

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    sqlContext.udf.register("getApkVersion", ApkVersionUtil.getApkVersion _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val s = sqlContext
        import s.implicits._

        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_APP_VERSION)
          .filter("dim_invalid_time is null")
          .select("version")
          .distinct()
          .registerTempTable("app_version_log")
        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_WEB_LOCATION)
          .filter("dim_invalid_time is null")
          .select("web_location_sk", "city")
          .distinct()
          .registerTempTable("web_location_log")
        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_CITY_DANGEROUS_LEVEL)
          .filter("dim_invalid_time is null")
          .select("city_name", "dangerous_level")
          .distinct()
          .registerTempTable("city_dangerous_level_log")
        DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_TERMINAL_USER)
          .filter("dim_invalid_time is null")
          .select("mac", "current_version", "web_location_sk", "open_time")
          .distinct()
          .registerTempTable("terminal_log")


        val inputDate = p.startDate
        val numOfDays = p.numOfDays
        val numOfPartition = 40

        val needToCalc = Array(1, 1, 1, 4, 7, 6, 10)
        val numOfDay = Array("one", "two", "three", "seven", "fourteen", "twenty", "thirty")
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
            .map(row => row.getString(0))
            .filter(_ != null) //.map(UserIdUtils.userId2Long)
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

            val sqlInfo = sqlContext.sql(s"SELECT mac,current_version,web_location_sk FROM terminal_log WHERE substr(open_time,0,10) = '$date2'")
            val sqlRDD = sqlInfo.map(rdd => (rdd.getString(0), rdd.getString(1)))
            sqlInfo.registerTempTable("terminal_log_new")

            //            val sqlInfo = s"SELECT mac,current_version FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$date2'"
            //            val sqlRDD = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.MTV_ACCOUNT, r => {
            //              (r.getString(1), r.getString(2))
            //            }, driver, url, user, password, (min, max), numOfPartition)

            //315版本
            val sqlRDD315 = sqlRDD
              .map(rdd => (rdd._1, ApkVersionUtil.getApkVersion(rdd._2)))
              .filter(_._2 == "3.1.5")
              .map(rdd => rdd._1)
              .distinct()

            //            val retentionNew = logUserID.intersection(sqlRDD315).count()
            //            val newUserNew = sqlRDD315.count().toInt
            //            val retentionRateNew = retentionNew.toDouble / newUserNew.toDouble


            sqlRDD315.toDF("mac").registerTempTable("new_user_mac")
            logUserID.intersection(sqlRDD315).toDF("mac").registerTempTable("retention_user_mac")
            sqlContext.sql(
              """
                |select dangerous_level,count(mac) as new_user
                |from
                |(select a2.mac,if(b2.dangerous_level IS NULL,'c',dangerous_level) as dangerous_level
                |from
                |(select a1.mac,b1.city
                |from
                |(select a.mac,b.web_location_sk
                |from new_user_mac a left join terminal_log_new b
                |on a.mac = b.mac) a1 left join web_location_log b1
                |on a1.web_location_sk = b1.web_location_sk) a2 left join city_dangerous_level_log b2
                |on a2.city = b2.city_name) a3
                |group by dangerous_level
              """.stripMargin)
              //.repartition(numOfPartition)
              .registerTempTable("dangerous_level_new") //对新增用户的mac映射出城市等级并统计数量

            sqlContext.sql(
              """
                |select dangerous_level,count(mac) as retention_user
                |from
                |(select a2.mac,if(b2.dangerous_level IS NULL,'c',dangerous_level) as dangerous_level
                |from
                |(select a1.mac,b1.city
                |from
                |(select a.mac,b.web_location_sk
                |from retention_user_mac a left join terminal_log_new b
                |on a.mac = b.mac) a1 left join web_location_log b1
                |on a1.web_location_sk = b1.web_location_sk) a2 left join city_dangerous_level_log b2
                |on a2.city = b2.city_name) a3
                |group by dangerous_level
              """.stripMargin)
              //.repartition(numOfPartition)
              .registerTempTable("dangerous_level_retention") //对留存用户的mac映射出城市等级并统计数量


            val resultRdd = sqlContext.sql(
              """
                |select a.dangerous_level,cast(a.new_user as int),round(b.retention_user/a.new_user,4) as retention_rate_new
                |from dangerous_level_new a join dangerous_level_retention b
                |on a.dangerous_level = b.dangerous_level
              """.stripMargin).map(rdd => (rdd.getString(0), rdd.getInt(1), rdd.getDouble(2)))

            //            if (p.deleteOld) {
            //              deleteSQL(date2, stmt1)
            //            }

            if (j == 0) {
              resultRdd.collect.foreach(rdd => {
                insertSQL(date2, rdd._1, rdd._2, rdd._3, stmt1)
              })

            } else {
              resultRdd.collect.foreach(rdd => {
                updateSQL(numOfDay(j), rdd._1, rdd._3, date2, stmt1)
              })
            }
          }
          logUserID.unpersist()
        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

  //  def getID(day: String, stmt: Statement): Array[Long] = {
  //    val sql = s"SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account` WHERE LEFT(openTime, 10) = '$day'"
  //    val id = stmt.executeQuery(sql)
  //    id.next()
  //    Array(id.getLong(1), id.getLong(2))
  //  }

  def insertSQL(date: String, dangerous_level: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO $resultTable (day,dangerous_level, new_user_num, one) VALUES('$date','$dangerous_level', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, dangerous_level: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE $resultTable SET $num = $retention WHERE day = '$date' and dangerous_level ='$dangerous_level'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date: String, stmt: Statement) = {
    val sql = s"DELETE FROM $resultTable WHERE day = '${date}'"
    stmt.execute(sql)
  }
}