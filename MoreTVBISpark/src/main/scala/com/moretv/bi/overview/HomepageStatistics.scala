package com.moretv.bi.overview

import java.util.Calendar
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import com.moretv.bi.util.SparkSetting
import org.apache.spark.sql.SQLContext
import java.lang.{Long => JLong, Double => JDouble}
import com.moretv.bi.util._

object HomepageStatistics extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(HomepageStatistics,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val path = "/mbi/parquet/homeview/"+date+"/*"
          //val sql = "select userId from log_data"
          val df = sqlContext.read.parquet(path).select("duration").filter("duration < 3600 and duration > 0").persist()
          val user_access = df.count()
          val s = sqlContext
          import s.implicits._
          val averageDuration = df.groupBy().avg("duration").first().getDouble(0)

          if(p.deleteOld){
            val sqlDelete = "DELETE FROM homepageStatistics WHERE day = ?"
            util.delete(sqlDelete,day)
          }

          val sqlInsert = "INSERT INTO homepageStatistics(day,user_access,averageDuration) VALUES(?,?,?)"
          util.insert(sqlInsert,day,new JLong(user_access),new JDouble(averageDuration))
          cal.add(Calendar.DAY_OF_MONTH, -1)
          df.unpersist()
        })
        util.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}

