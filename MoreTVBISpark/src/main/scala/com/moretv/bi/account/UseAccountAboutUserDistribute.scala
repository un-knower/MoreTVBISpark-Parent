package com.moretv.bi.account

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.Pattern

import com.moretv.bi.user.UserAgeStatistics._
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.constant.Tables
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2015/4/18.
  */
object UseAccountAboutUserDistribute extends BaseClass with QueryMaxAndMinIDUtil {

  def main(args: Array[String]) {
    config.setAppName("UseAccountAboutUserDistribute")
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val yesterdayCN = DateFormatUtils.toDateCN(p.startDate, -1)

        //处理日志取出accountId和userId
        val programMap = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.MTVACCOUNT)
          .filter("event = 'login' and accountId != 0")
          .select("userId", "accountId")
          .map(e => (e.getString(0), e.getInt(1) + ""))
          .distinct()

        val db = DataIO.getMySqlOps("moretv_bi_mysql")
        val url = db.prop.getProperty("url")
        val driver = db.prop.getProperty("driver")
        val user = db.prop.getProperty("user")
        val password = db.prop.getProperty("password")
        val sqlInfo = "SELECT userid,accountid FROM `accountidAndUserid` WHERE ID >= ? AND ID <= ?"

        val (min, max) = db.queryMaxMinID(Tables.USERIDUSINGACCOUNT, "id")


        val userIdRDD = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.USERIDUSINGACCOUNT,
          r => r.getString(1) + " " + r.getString(2), driver, url, user, password, (min, max), 300
        )
          .map(x => splitsLog(x))
          .filter(_ != null)
          .distinct


        val resultRDD = programMap.subtract(userIdRDD)

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val oldSql = s"delete from accountidAndUserid where day = '$yesterdayCN'"
          val resultSql = s"delete from accountNumsToUserNum where day = '$yesterdayCN'"
          util.delete(oldSql)
          util.delete(resultSql)
        }
        //存储数据accountId和userId
        val sql = "INSERT INTO bi.accountidAndUserid(day,userid,accountid) values(?,?,?)"
        val result = resultRDD.collect()
        var index = 0
        result.foreach(x => {
          util.insert(sql, yesterdayCN, x._1, x._2)
          index = index + 1
        })

        val sqlInfo1 = "SELECT userid,count(accountid) FROM `accountidAndUserid` WHERE ID >= ? AND ID <= ? group by userid"

        //取出一个usrid对应几个account
        val userIdtoaccountidRDD = MySqlOps.getJdbcRDD(sc, sqlInfo1, Tables.USERIDUSINGACCOUNT,
          r => r.getString(1) + " " + r.getString(2), driver, url, user, password, (min, max), 300
        )
          .map(x => splitsLog(x))
          .filter(_ != null)
          .map(x => (x._2, x._1)).countByKey()

        //保存累计账户数
        val resultsql = "INSERT INTO bi.accountNumsToUserNum(day,account_num,user_num) values(?,?,?)"
        userIdtoaccountidRDD.foreach(x => {
          util.insert(resultsql, yesterdayCN, new Integer(x._1), new Integer(x._2.toInt))
        })
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def splitsLog(log: String) = {
    val matcher = log.split(" ")
    if (matcher.length == 2) {
      (matcher(0), matcher(1))
    } else null
  }

}
