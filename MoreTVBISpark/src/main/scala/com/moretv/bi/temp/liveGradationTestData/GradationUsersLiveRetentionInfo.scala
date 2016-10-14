package com.moretv.bi.temp.liveGradationTestData

import java.sql.{DriverManager, Statement}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/10/11.
 * 计算小鹰直播10月8号和9号升级至3.1.1版本的用户,每日的直播频道播放情况
 */
object GradationUsersLiveRetentionInfo extends SparkSetting {
  private val tableName = "medusa_live_gradation_test_retention_info"
  val util = DataIO.getMySqlOps("medusa_mysql")

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val needToCalc = Array(1, 1, 1, 1, 1, 1, 1)
        val numOfDay = Array("one", "two", "three", "four", "five", "six", "seven")
        val gradationUsersDir = "/log/medusa/temple/gradationUsers"
        val parentDir = "/log/medusa/parquet/"
        val logType = "live"
        sqlContext.read.parquet(gradationUsersDir).registerTempTable("log_gradation")
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i => {
          val c = Calendar.getInstance()
          c.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE))
          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)

          val date = s"${logDay}/"
          val inputDir = s"${parentDir}${date}${logType}"
          sqlContext.read.parquet(inputDir).select("userId", "event", "duration",
            "liveType", "apkVersion").filter("liveType='live'").
            registerTempTable("log_live")

          /**
           * 计算灰度升级用户的直播次数与直播人数
           */
          val userRdd = sqlContext.sql(
            """select distinct a.userId
              |from log_live as a join log_gradation as b on a.userId=b.userId
              |where a.event='startplay' and a.liveType = 'live'""".stripMargin).
            map(e => e.getString(0))
          calendar.add(Calendar.DAY_OF_MONTH, 1)
          Class.forName("com.mysql.jdbc.Driver")
          val connection = DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
            "bi", "mlw321@moretv")
          val stmt = connection.createStatement()
          (0 until needToCalc.length).foreach(j => {
            c.add(Calendar.DAY_OF_MONTH, -needToCalc(j))
            val logDay1 = DateFormatUtils.readFormat.format(c.getTime)
            val sqlDay = DateFormatUtils.toDateCN(logDay1, -1)
            val date1 = s"${logDay1}/"
            val inputDir1 = s"${parentDir}${date1}/${logType}"
            sqlContext.read.parquet(inputDir1).select("userId", "event", "duration",
              "liveType", "apkVersion").filter("liveType='live'").
              registerTempTable("log_live1")
            val userRdd1 = sqlContext.sql(
              """select distinct a.userId
                |from log_live1 as a join log_gradation as b on a.userId=b.userId
                |where a.event='startplay' and a.liveType = 'live'""".stripMargin).
              map(e => e.getString(0))

            val retentionUser = userRdd.intersection(userRdd1).count()
            val newUser = userRdd1.count()
            if (j == 0) {
              insertSQL(sqlDay, newUser, retentionUser,stmt)
            } else {
              updateSQL(numOfDay(j),retentionUser, sqlDay,stmt)
            }
          })

        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

  def insertSQL(date: String, count: Long, retention: Long,stmt: Statement) = {
    val sql = s"INSERT INTO ${tableName} (DAY, new_user_num, one) VALUES('$date', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, retention: Double, date: String,stmt: Statement) = {
    val sql = s"UPDATE ${tableName} SET $num = $retention WHERE DAY = '$date'"
    stmt.executeUpdate(sql)
  }
}
