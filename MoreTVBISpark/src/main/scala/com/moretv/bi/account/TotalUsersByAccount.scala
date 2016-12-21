package com.moretv.bi.account

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.Pattern

import com.moretv.bi.account.UseAccountAboutUserDistribute._
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

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
        val id = queryID("id", "useridByUsingAccount", "jdbc:mysql://10.10.2.15:3306/bi?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");


        config.setAppName("TotalUsersByAccount")

        val programMap = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.MTVACCOUNT)
          .filter("event = 'login'")
          .select("userId")
          .map(e => e.getString(0)).distinct()


        val userIdRDD = new JdbcRDD(sc, () => {
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/bi?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
        },
          "SELECT userid FROM `useridByUsingAccount` WHERE ID >= ? AND ID <= ?",
          id(1), id(0), 10,
          r => r.getString(1)).distinct()
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
        val sql2 = "INSERT INTO bi.totalUsersByUsingAccount(day,totalusers) select '" + yesterdayCN + "', count(0) from bi.useridByUsingAccount where day <= '" + yesterdayCN + "'"
        util.insert(sql2)
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
}
