package com.moretv.bi.overview

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by laishun on 15/10/9.
  */
object HelpManualStatics extends BaseClass with DateUtil {

  def main(args: Array[String]) {
    config.setAppName("HelpManualStatics")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val resultRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PAGEVIEW)
          .filter("page = 'helpManual'")
          .select("date", "path", "userId")
          .map(e => (e.getString(0), e.getString(1), e.getString(2)))
          .map(e => (getKeys(e._1, e._2), e._3))
          .filter(e => e._1 != null)
          .persist(StorageLevel.MEMORY_AND_DISK)

        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from helpmanual where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO helpmanual(year,month,day,enter_code,enter_name,user_num,access_num) VALUES(?,?,?,?,?,?,?)"
        userNum.foreach(x => {
          util.insert(sql, new Integer(x._1._1), new Integer(x._1._2), x._1._3, x._1._4, x._1._5, new Integer(x._2.toInt), new Integer(accessNum(x._1).toInt))
        })
        resultRDD.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date: String, path: String) = {
    val year = date.substring(0, 4)
    val month = date.substring(5, 7).toInt

    if (path == "feedback") {
      (year, month, date, "feedback", "从设置功能进入")
    } else if (path == "network_check") {
      (year, month, date, "network_check", "从网络诊断进入")
    } else if (path == "playerror") {
      (year, month, date, "playerror", "从播放失败进入")
    } else {
      null
    }
  }
}
