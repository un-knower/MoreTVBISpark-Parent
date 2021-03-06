package com.moretv.bi.login

import java.lang.{Long => JLong}
import java.util.regex.Pattern

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.account.AccountAccess._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2015/2/5.
  */
object YunOSLoginUser extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val inputDate = p.startDate

        val logRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG)
          .select("version", "mac")
          .map(row => if (matchLog(row.getString(0))) row.getString(1) else null)
          .filter(_ != null).cache()

        val loginNum = logRdd.count()

        val userNum = logRdd.distinct().count()

        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if (p.deleteOld) {
          val oldSql = s"delete from bi.yunos_login_user where day = '$day'"
          db.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO bi.yunos_login_user(day,user_num,access_num) VALUES(?,?,?)"
        db.insert(sql, day, new JLong(userNum), new JLong(loginNum))
        db.destory()
        logRdd.unpersist()

      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

  def matchLog(version: String) = {
    if (version != null) {
      if (version.contains("YunOS") || version.contains("Alibaba")) true else false
    } else false
  }

}
