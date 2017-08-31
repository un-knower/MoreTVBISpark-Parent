package com.moretv.bi.report.medusa.dataAnalytics

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

object MigrationByboshilianDayRetentionRate extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlC = sqlContext
        import sqlC.implicits._
        val inputDate = p.startDate
        val numOfDays = p.numOfDays
        val numOfPartition = 40

        val needToCalc = Array(1, 1, 1, 1, 1,
                               1, 1)
        val numOfDay = Array("d1", "d2", "d3", "d4", "d5",
                             "d6", "d7")
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
          userLog.select("userId").registerTempTable("login_user")
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

            val db = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
            val dbDriver = db.prop.getProperty("driver")
            val dbUrl = db.prop.getProperty("url")
            val dbUser = db.prop.getProperty("user")
            val dbPassword = db.prop.getProperty("password")
            val dbConnection = DriverManager.getConnection(dbUrl,dbUser, dbPassword)
            val stmt1 = dbConnection.createStatement()
            val id = getID(date2, stmt1)
            val min = id(0)
            val max = id(1)
            // 获取特殊设备型号新增的用户的留存数据
            val sqlInfo = s"SELECT user_id, promotion_channel FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$date2' and " +
              s"promotion_channel ='boshilian'"
            MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.MTV_ACCOUNT, r => {
              (r.getString(1),r.getString(2))}, dbDriver, dbUrl, dbUser, dbPassword, (min, max), numOfPartition).distinct().toDF("userId","promotion_channel").registerTempTable("new_user")

            val retentionRdd = sqlContext.sql(
              """
                |select promotion_channel, count(distinct a.userId) as retention_user
                |from new_user as a
                |join login_user as b
                |on a.userId = b.userId
                |group by promotion_channel
              """.stripMargin).map(e=>(e.getString(0),e.getLong(1)))

            val newUserRdd = sqlContext.sql(
              """
                |select promotion_channel, count(distinct userId) as new_user
                |from new_user
                |group by promotion_channel
              """.stripMargin).map(e=>(e.getString(0),e.getLong(1)))

            val mergerDF = newUserRdd.join(retentionRdd).map(e=>(e._1,e._2._1,e._2._2)).collect()


            println("****************The num of retention is: "+mergerDF.length)

            if (j == 0 ) {
              mergerDF.foreach(e=>{
                var retentionRateAll = 0.00
                if(e._2 !=0 ){
                  retentionRateAll = e._3.toDouble / e._2.toDouble
                }
                insertSQL(date2, e._1, e._2.toInt, retentionRateAll, stmt)

              })
            } else {
              mergerDF.foreach(e=>{
                var retentionRateAll = 0.00
                if(e._2 !=0 ){
                  retentionRateAll = e._3.toDouble / e._2.toDouble
                }
                updateSQL(numOfDay(j), e._1, retentionRateAll, date2, stmt)
              })
            }
          }
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

  def insertSQL(date: String, typeInfo: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO medusa.`mtv_account_boshilian_filter_retention` (day,type_info, new_num, d1) VALUES('$date','$typeInfo', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, typeInfo: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.`mtv_account_boshilian_filter_retention` SET $num = $retention WHERE day = '$date' and type_info ='$typeInfo'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.`mtv_account_boshilian_filter_retention` WHERE day = ${date}"
    stmt.execute(sql)
  }
}