package com.moretv.bi.retention

import java.sql.{DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

object PromotionChannelDayRetentionRateV2 extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val numOfDays = p.numOfDays
        val promotionChannels = p.paramMap("promotionChannels").split("@@").mkString("','")
        val pcs = s"'$promotionChannels'"

        val needToCalc = Array(1, 1, 1, 1, 1,
                               1, 1, 7, 16)
        val numOfDay = Array("d1", "d2", "d3", "d4", "d5",
                             "d6", "d7", "d14", "d30")
        val format = new SimpleDateFormat("yyyy-MM-dd")
        val readFormat = new SimpleDateFormat("yyyyMMdd")
        val date = readFormat.parse(inputDate)
        val s = sqlContext
        import s.implicits._

        val calendar = Calendar.getInstance()
        calendar.setTime(date)

        for (i <- 0 until numOfDays) {
          val c = Calendar.getInstance()
          c.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE) - 1)
          val inputDate = readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH, 1)
          val userLog = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, inputDate)
          userLog.select("userId").distinct().registerTempTable("login_user")
          Class.forName("com.mysql.jdbc.Driver")
          // 创建插入数据库连接
          val insertDB = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
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

            // 获取特殊设备型号新增的用户的留存数据
            val dbKey = DataBases.MORETV_MEDUSA_MYSQL
            val sqlMinMaxId = s"select min(id),max(id) from mtv_account_migration where openTime between '$date2 00:00:00' and '$date2 23:59:59'"
            val sqlData = s"select user_id,promotion_channel from mtv_account_migration where id >= ? and id <= ? and openTime between '$date2 00:00:00' and '$date2 23:59:59' and promotion_channel in ($pcs)"
            MySqlOps.getJdbcRDD(sc,dbKey,sqlMinMaxId,sqlData,100,rs => (rs.getString(1),rs.getString(2))).toDF("userId","promotionChannel").registerTempTable("new_user")

            val retentionRdd = sqlContext.sql(
              """
                |select promotionChannel, count(distinct a.userId) as retention_user
                |from new_user as a
                |join login_user as b
                |on a.userId = b.userId
                |group by promotionChannel
              """.stripMargin).map(e=>(e.getString(0),e.getLong(1)))

            val newUserRdd = sqlContext.sql(
              """
                |select promotionChannel, count(distinct userId) as new_user
                |from new_user
                |group by promotionChannel
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


  def insertSQL(date: String, typeInfo: String, count: Int, retention: Double, stmt: Statement) = {
    val sql = s"INSERT INTO medusa.`promotion_channel_day_retention_rate_v2` (day,type_info, new_num, d1) VALUES('$date','$typeInfo', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num: String, typeInfo: String, retention: Double, date: String, stmt: Statement) = {
    val sql = s"UPDATE medusa.`promotion_channel_day_retention_rate_v2` SET $num = $retention WHERE day = '$date' and type_info ='$typeInfo'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date: String, stmt: Statement) = {
    val sql = s"DELETE FROM medusa.`promotion_channel_day_retention_rate_v2` WHERE day = ${date}"
    stmt.execute(sql)
  }
}