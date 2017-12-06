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

object RealUserDayRetentionRate extends BaseClass{


  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val numOfDays = p.numOfDays
        val numOfPartition = 40

        val needToCalc = Array(1,1,1,1,1,1,1,7,16)
        val numOfDay = Array("one","two","three","four","five","six","seven","fourteen","thirty")
        val format = new SimpleDateFormat("yyyy-MM-dd")
        val readFormat = new SimpleDateFormat("yyyyMMdd")
        val date = readFormat.parse(inputDate)

        val calendar = Calendar.getInstance()
        calendar.setTime(date)

        for(i<-0 until numOfDays){
          val c = Calendar.getInstance()
          c.set(calendar.get(Calendar.YEAR),calendar.get(Calendar.MONTH),calendar.get(Calendar.DATE)-1)
          val inputDate = readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH,1)
          val userLog = DataIO.getDataFrameOps.getDF(sc,p.paramMap,LOGINLOG,LogTypes.LOGINLOG,inputDate)
          val logUserID = userLog.select("mac").map(row => row.getString(0)).filter(_ != null).
            map(UserIdUtils.userId2Long).distinct().cache()
          Class.forName("com.mysql.jdbc.Driver")
          val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          val driver = db.prop.getProperty("driver")
          val url = db.prop.getProperty("url")
          val user = db.prop.getProperty("user")
          val password = db.prop.getProperty("password")
          val connection = DriverManager.getConnection(url,user, password)
          val stmt = connection.createStatement()

          // 创建插入数据库连接
          val insertDB = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
          val url1 = insertDB.prop.getProperty("url")
          val user1 = insertDB.prop.getProperty("user")
          val password1 = insertDB.prop.getProperty("password")
          val connection1 = DriverManager.getConnection(url1,user1,password1)
          val stmt1 = connection1.createStatement()

          for(j<- 0 until needToCalc.length){
            c.add(Calendar.DAY_OF_MONTH,-needToCalc(j))
            val date2 = format.format(c.getTime)
            val id = getID(date2,stmt)
            val min = id(0)
            val max =id(1)
            val sqlInfo = s"SELECT mac FROM `mtv_account_migration` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$date2'"
            val sqlRDD = MySqlOps.getJdbcRDD(sc,sqlInfo,"mtv_account_migration",r=>{r.getString(1)},
            driver,url,user,password,(min,max),numOfPartition).
              map(UserIdUtils.userId2Long).distinct()

            val retention = logUserID.intersection(sqlRDD).count()
            val newUser = sqlRDD.count().toInt
            val retentionRate = retention.toDouble/newUser.toDouble
            if(p.deleteOld){
              deleteSQL(date2,stmt1)
            }
            if(j==0){
              insertSQL(date2,newUser,retentionRate,stmt1)
            }else{
              updateSQL(numOfDay(j),retentionRate,date2,stmt1)
            }
          }
          logUserID.unpersist()
        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }
  def getID(day: String, stmt: Statement): Array[Long]={
    val sql = s"SELECT MIN(id),MAX(id) FROM medusa.`mtv_account_migration` WHERE LEFT(openTime, 10) = '$day'"
    val id = stmt.executeQuery(sql)
    id.next()
    Array(id.getLong(1),id.getLong(2))
  }

  def insertSQL(date: String, count: Int, retention: Double,stmt: Statement) ={
    val sql = s"INSERT INTO bi.`user_retetion_day2` (DAY, new_user_num, ONE) VALUES('$date', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num:String, retention:Double, date:String, stmt: Statement)={
    val sql = s"UPDATE bi.`user_retetion_day2` SET $num = $retention WHERE DAY = '$date'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date:String,stmt:Statement) = {
    val sql = s"DELETE FROM bi.`user_retetion_day2` WHERE DAY = ${date}"
    stmt.execute(sql)
  }
}