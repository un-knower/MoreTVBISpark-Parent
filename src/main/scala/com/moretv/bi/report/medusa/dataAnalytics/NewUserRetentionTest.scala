package com.moretv.bi.report.medusa.dataAnalytics

import java.sql.{DriverManager, Statement}

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by xiajun on 2017/7/31.
  */
object NewUserRetentionTest extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContextTmp = sqlContext
        import sqlContextTmp.implicits._
        val numOfPartition = 10

        val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val driver = db.prop.getProperty("driver")
        val url = db.prop.getProperty("url")
        val user = db.prop.getProperty("user")
        val password = db.prop.getProperty("password")
        val connection = DriverManager.getConnection(url,user, password)
        val stmt = connection.createStatement()

        val id = getID(stmt)
        val min = id(0)
        val max =id(1)
        val sqlInfo = s"SELECT user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ?"
        val userDF = MySqlOps.getJdbcRDD(sc,sqlInfo,Tables.MTV_ACCOUNT,r=>{r.getString(1)},
          driver,url,user,password,(min,max),numOfPartition).toDF("userId")

      }
      case None => {}
    }
  }

  def getID(stmt: Statement): Array[Long]={
    val sql = s"SELECT MIN(id),MAX(id) FROM tvservice.`mtv_account`"
    val id = stmt.executeQuery(sql)
    id.next()
    Array(id.getLong(1),id.getLong(2))
  }


}
