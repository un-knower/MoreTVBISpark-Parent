package com.moretv.bi.operation


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
object SubjectCollectUsage extends BaseClass with DateUtil {

  def main(args: Array[String]) {
    config.setAppName("SubjectCollectUsage")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val resultRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.COLLECT)
          .filter("collectType = 'subject'")
          .select("date", "event", "userId")
          .map(e => (e.getString(0), e.getString(1), e.getString(2)))
          .map(e => (getKeys(e._1, e._2), e._3)).persist(StorageLevel.MEMORY_AND_DISK)

        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from subject_collect_usage where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO subject_collect_usage(year,month,day,operation,user_num,operate_num) VALUES(?,?,?,?,?,?)"
        userNum.foreach(x => {
          util.insert(sql, new Integer(x._1._1), new Integer(x._1._2), x._1._3, x._1._4, new Integer(x._2.toInt), new Integer(accessNum(x._1).toInt))
        })
        resultRDD.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date: String, event: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(date))
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1

    (year, month, date, event)
  }
}
