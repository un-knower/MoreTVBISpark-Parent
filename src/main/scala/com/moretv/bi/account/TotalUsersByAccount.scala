package com.moretv.bi.account

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by Will on 2015/4/18.
  */
object TotalUsersByAccount extends BaseClass with QueryMaxAndMinIDUtil {
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val yesterdayCN = DateFormatUtils.toDateCN(p.startDate, -1)
        
        config.setAppName("TotalUsersByAccount")

        val programMap = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.MTVACCOUNT)
          .filter("event = 'login'")
          .select("userId")
          .map(e => e.getString(0))
          .distinct

        val db = DataIO.getMySqlOps("moretv_bi_mysql")
        val url = db.prop.getProperty("url")
        val driver = db.prop.getProperty("driver")
        val user = db.prop.getProperty("user")
        val password = db.prop.getProperty("password")
        val sqlInfo = "select userid from `useridByUsingAccount` where ID >= ? AND ID <= ?"

        val (min, max) = db.queryMaxMinID(Tables.USERIDUSINGACCOUNT, "id")

        val userIdRDD = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.USERIDUSINGACCOUNT,
          r => (r.getString(1)), driver, url, user, password, (min, max), 300)
          .distinct

        val resultRDD = programMap.subtract(userIdRDD)

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val oldSql = s"delete from useridByUsingAccount where day = '$yesterdayCN'"
          val resultSql = s"delete from totalUsersByUsingAccount where day = '$yesterdayCN'"
          util.delete(oldSql)
          util.delete(resultSql)
        }
        val sql = "INSERT INTO bi.useridByUsingAccount(day,userid) values(?,?)"
        val result = resultRDD.collect()
        result.foreach(x => {
          util.insert(sql, yesterdayCN, x)
        })
        //保存累计账户数
        val sql2 = "INSERT INTO bi.totalUsersByUsingAccount(day,totalusers) select '" + yesterdayCN +
          "', count(0) from bi.useridByUsingAccount where day <= '" + yesterdayCN + "'"
        util.insert(sql2)
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
}
