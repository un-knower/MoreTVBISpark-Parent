package com.moretv.bi.login

import java.lang.{Long => JLong}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by laishun on 15/10/9.
  */
object TotalAndAvgTime extends BaseClass with DateUtil{
  def main(args: Array[String]): Unit = {
    config.setAppName("TotalAndAvgTime")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        DataIO.getDataFrameOps.getDF(sc, p.paramMap, MERGER, LogTypes.EXIT,p.startDate).registerTempTable("log_data")
        val result = sqlContext.sql("select count(distinct userId),count(userId),sum(duration) from log_data where duration between 0 and 54000")
          .collect()

        //save date
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        val date = DateFormatUtils.toDateCN(p.startDate, -1)
        if (p.deleteOld) {
          val oldSql = s"delete from total_duration_user where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO total_duration_user(year,month,day,weekstart_end,user_num,login_num,total_duration) VALUES(?,?,?,?,?,?,?)"
        result.foreach(row =>{
          val keys = getKeys(date)
          util.insert(sql,new Integer(keys._1),new Integer(keys._2),keys._3,keys._4,new JLong(row.getLong(0)),
            new JLong(row.getLong(1)),new JLong(row.getLong(2)))
        })

      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String)={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)

    (year,month,date,week)
  }
}
