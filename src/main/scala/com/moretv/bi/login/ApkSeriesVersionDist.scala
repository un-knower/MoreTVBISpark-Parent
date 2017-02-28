package com.moretv.bi.login

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.account.AccountAccess._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/2/16.
  */
object ApkSeriesVersionDist extends BaseClass {

  val regex = "^[\\w\\.]+$".r

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate

        val logRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG)
          .select("version", "mac")
          .map(row =>
            if (row.getString(0) == null)
              ("null", row.getString(1))
            else (row.getString(0), row.getString(1)))
          .cache()

        val loginNums = logRdd.countByKey()
        val userNums = logRdd.distinct().countByKey()

        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if (p.deleteOld) {
          val sqlDelete = "delete from apk_version where date = ?"
          db.delete(sqlDelete, day)
        }

        val sqlInsert = "insert into apk_version(date,apk_version,usernum,loginnum) values(?,?,?,?)"
        userNums.foreach(x => {
          val version = x._1
          regex findFirstMatchIn version match {
            case Some(v) => {
              val usernum = x._2
              val loginnum = loginNums(version)
              db.insert(sqlInsert, day, version, new Integer(usernum.toInt), new Integer(loginnum.toInt))
            }
            case None =>
          }

        })
        db.destory()
        logRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
