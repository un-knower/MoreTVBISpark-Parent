package com.moretv.bi.login

import java.sql.SQLException

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/2/16.
  * 统计各版本的登录时段分布
  */
object LoginPeriodByApkSeriesVersion extends BaseClass {

  val regex = "^[\\w\\.]+$".r

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(LoginPeriodByApkSeriesVersion, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val inputDate = p.startDate

        DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG)
          .select("date", "datetime", "version", "mac").registerTempTable("log_data")

        sqlContext.udf.register("getPeriod", (x: String) => x.substring(11, 13))

        val result = sqlContext.sql("select version,getPeriod(datetime),count(distinct mac) from log_data " +
          "group by getPeriod(datetime),version").collectAsList()


        val db = DataIO.getMySqlOps("moretv_bi_mysql")
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if (p.deleteOld) {
          val sqlDelete = "delete from login_period_version_distribution where day = ?"
          db.delete(sqlDelete, day)
        }

        val sqlInsert = "insert into login_period_version_distribution(day,version,period,user_num) values(?,?,?,?)"
        result.foreach(row => {
          try {
            db.insert(sqlInsert, day, row.get(0), row.get(1), row.get(2))
          } catch {
            case e: SQLException =>
              if (e.getMessage.contains("Data too long"))
                println(e.getMessage)
              else throw new RuntimeException(e)
            case e: Exception => throw new RuntimeException(e)
          }
        })
        db.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
