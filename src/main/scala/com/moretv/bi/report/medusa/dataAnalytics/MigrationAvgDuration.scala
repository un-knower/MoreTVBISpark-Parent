package com.moretv.bi.report.medusa.dataAnalytics

import java.sql.{DriverManager, Statement}
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by xiajun on 2017/8/2.
  */
object MigrationAvgDuration extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlC = sqlContext
        import sqlC.implicits._
        Class.forName("com.mysql.jdbc.Driver")
        val insertDB = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val driver =insertDB.prop.getProperty("driver")
        val url = insertDB.prop.getProperty("url")
        val user = insertDB.prop.getProperty("user")
        val password = insertDB.prop.getProperty("password")
        val connection = DriverManager.getConnection(url, user, password)
        val stmt = connection.createStatement()
        val numOfPartition = 40

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)

        val cal1 = Calendar.getInstance()
        cal1.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date)
          val date1 = DateFormatUtils.readFormat.format(cal1.getTime)
          cal1.add(Calendar.DAY_OF_MONTH, -1)
          cal.add(Calendar.DAY_OF_MONTH, -1)

          val id = getID(insertDate, stmt)
          val min = id(0)
          val max = id(1)
          val sqlInfo = s"SELECT user_id FROM `mtv_account_migration_vice` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$insertDate'"
          MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.MTV_ACCOUNT_MIGRATOPN_VICE, r => {
            (r.getString(1))}, driver, url, user, password, (min, max), numOfPartition).distinct().toDF("userId").registerTempTable("new_user")
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MERGER, LogTypes.PLAYVIEW, date1).registerTempTable("play")

          val avgDuration = sqlContext.sql(
            """
              |select sum(duration)/count(distinct a.userId)
              |from play as a
              |join new_user as b
              |on a.userId = b.userId
              |where event != 'startplay' and duration between 0 and 10800
            """.stripMargin).map(e=>e.getDouble(0)).collect()

          val insertSql = "insert into mtv_account_migration_avg_duration(day, avg_duration) values(?,?)"
          val deleteSql = "delete from mtv_account_migration_avg_duration where day = ?"

          if(p.deleteOld){
            insertDB.delete(deleteSql,insertDate)
          }

          avgDuration.foreach(k=>{
            insertDB.insert(insertSql,insertDate,k)
          })

        })



      }
      case None => {

      }
    }
  }

  def getID(day: String, stmt: Statement): Array[Long] = {
    val sql = s"SELECT MIN(id),MAX(id) FROM medusa.`mtv_account_migration_vice` WHERE LEFT(openTime, 10) = '$day'"
    val id = stmt.executeQuery(sql)
    id.next()
    Array(id.getLong(1), id.getLong(2))
  }

}
