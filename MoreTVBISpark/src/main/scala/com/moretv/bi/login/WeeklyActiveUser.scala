package com.moretv.bi.login

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
  * Created by Will on 2016/2/16.
  */
object WeeklyActiveUser extends BaseClass {

  def main(args: Array[String]): Unit = {

    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val days = getInputPaths(-p.offset)
        val mondayCN = DateFormatUtils.toDateCN(days(0))
        val sundayCN = DateFormatUtils.toDateCN(days(6))
        val weekStartEnd = mondayCN + "~" + sundayCN
        val logRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, days)
          .select("mac")
        val userNum = logRdd.distinct().count().toInt

        val dbTvservice = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)

        val sqlNewNum = "select count(distinct mac) from mtv_account " +
          s"where openTime between '$mondayCN 00:00:00' and '$sundayCN 23:59:59'"

        val newNum = dbTvservice.selectOne(sqlNewNum)(0).toString.toInt
        dbTvservice.destory()
        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        if (p.deleteOld) {
          val sqlDelete = "delete from login_detail_week where day = ?"
          db.delete(sqlDelete, sundayCN)
        }

        val sqlLastTotal = "select totaluser_num from login_detail_week order by id desc limit 1"
        val lastTotalUser = db.selectOne(sqlLastTotal)(0).toString.toInt
        val sqlInsert = "insert into login_detail_week(weekstart_end,day,new_num,active_num,totaluser_num) values(?,?,?,?,?)"
        val activeNum = userNum - newNum
        val totalUser = lastTotalUser + newNum
        db.insert(sqlInsert, weekStartEnd, sundayCN, new Integer(newNum), new Integer(activeNum), new Integer(totalUser))
        db.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

  def getInputPaths(offset: Int) = {

    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
    val sunday = format.format(cal.getTime)
    cal.add(Calendar.WEEK_OF_YEAR, offset)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    val monday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY)
    val tuesday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.WEDNESDAY)
    val wednesday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY)
    val thursday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY)
    val friday = format.format(cal.getTime)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY)
    val saturday = format.format(cal.getTime)

    Array(monday, tuesday, wednesday, thursday, friday, saturday, sunday)

  }
}
