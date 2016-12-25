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

/**
  * Created by laishun on 15/10/9.
  */
object SubjectCollectQuantity extends BaseClass with DateUtil {

  def main(args: Array[String]) {
    config.setAppName("SubjectCollectQuantity")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val resultRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.COLLECT)
          .filter("collectType like '%subject%'")
          .select("date", "collectContent", "event")
          .map(e => (e.getString(0), e.getString(1), e.getString(2)))
          .map(e => (getKeys(e._1, e._2), e._3))
          .groupByKey()
          .map(e => (e._1, countOKAndCancle(e._2))).collect()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from subject_collect_quantity where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO subject_collect_quantity(year,month,day,code,name,collect,uncollect) VALUES(?,?,?,?,?,?,?)"
        resultRDD.foreach(x => {
          util.insert(sql, new Integer(x._1._1), new Integer(x._1._2), x._1._3, x._1._4, x._1._5, new Integer(x._2._1), new Integer(x._2._2))
        })
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date: String, collectContent: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(date))
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1

    var subjectName = CodeToNameUtils.getSubjectNameBySid(collectContent)
    if (subjectName == null) subjectName = collectContent

    (year, month, date, collectContent, subjectName)
  }

  def countOKAndCancle(events: Iterable[String]) = {
    var ok = 0
    var cancle = 0
    events.foreach(x => {
      if (x == "ok") {
        ok = ok + 1
      } else if (x == "cancel") {
        cancle = cancle + 1
      }
    })
    (ok, cancle)
  }
}
