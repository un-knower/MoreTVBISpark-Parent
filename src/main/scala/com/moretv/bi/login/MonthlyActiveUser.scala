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
object MonthlyActiveUser extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val days = getInputDates(p.offset)
        val inputPaths = days.map(date => DataIO.getDataFrameOps.getPath(LOGINLOG,LogTypes.LOGINLOG,date))
        val logRdd = sqlContext.read.load(inputPaths:_*).select("mac").cache()
        val loginNum = logRdd.count().toInt
        val userNum = logRdd.distinct().count().toInt

        val format = new SimpleDateFormat("yyyyMM")
        val cal = Calendar.getInstance()
        cal.add(Calendar.DAY_OF_MONTH,-p.offset)
        val month = format.format(cal.getTime)
        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        if(p.deleteOld){
          val sqlDelete = "delete from login_detail_month where month = ?"
          db.delete(sqlDelete,month)
        }

        val sqlInsert = "insert into login_detail_month(month,active_num,login_num) values(?,?,?)"
        db.insert(sqlInsert,month,new Integer(userNum),new Integer(loginNum))
        db.destory()
        logRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

  def getInputDates(offset:Int) = {

    val format = new SimpleDateFormat("yyyyMMdd")
    val today = Calendar.getInstance()
    today.add(Calendar.MONTH,-offset+1)
    today.set(Calendar.DAY_OF_MONTH, 1)
    val cal = Calendar.getInstance()
    cal.add(Calendar.MONTH,-offset)
    cal.set(Calendar.DAY_OF_MONTH, 1)
    val days = today.get(Calendar.DAY_OF_YEAR) - cal.get(Calendar.DAY_OF_YEAR)
    (0 until days).map(i => {
      cal.add(Calendar.DAY_OF_YEAR,1)
      format.format(cal.getTime)
    })
  }
}
