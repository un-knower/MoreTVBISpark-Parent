package com.moretv.bi.report.medusa.userDevelop

import java.text.SimpleDateFormat
import java.util.Calendar

import java.lang.{Long => JLong, Integer => JInt}
import com.moretv.bi.util.{Params, DBOperationUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 11/25/16.
  */

/**
  * 终端型号月统计
  *
  */
object ProductModelUserStat extends BaseClass {

  private var loadDateFormat = "yyyyMMdd"

  private var endDateForamt = "yyyy-MM-dd"

  private var startDateFormat = "yyyy-MM-dd"


  private val tableName = "productmodel_interval_stat"

  private val fields = "intervals,interval_type,productModel,uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?)"

  private val deleteSql = s"delete from $tableName where interval = ?"


  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val dateArr = setDateValues("month", p.startDate,p)


        sqlContext.read.parquet(dateArr._1)
          .filter(s"date between '${dateArr._2}' and '${dateArr._3}'")
          .filter("length(productModel) < 100")
          .selectExpr("month(date) as intervals", "productModel", "userId")
          .registerTempTable("log_data")


        val df = sqlContext.sql(
          """
            |select intervals, productModel ,count(distinct userId) as uv
            |from log_data
            |group by intervals, productModel
          """.stripMargin)
          .repartition(400)

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        df.collect.foreach(w => {
          val interval = w.getInt(0)
          val interval_type = "month"
          val productModel = w.getString(1)
          val uv = w.getLong(2)

          util.insert(insertSql, new JInt(interval), interval_type, productModel, new JLong(uv))

        })

      }
      case None => {

      }
    }
  }

  /**
    *
    * @param dateType
    * @param startDate
    * @return
    */
  def setDateValues(dateType: String, startDate: String,p:Params): (String, String, String) = {

    val format1 = new SimpleDateFormat("yyyyMMdd")
    val format2 = new SimpleDateFormat("yyyy-MM-dd")

    val cal = Calendar.getInstance

    if (startDate != "") {
      cal.setTime(format1.parse(startDate))
    }

    dateType match {
      case "month" => {

        val year = cal.get(Calendar.YEAR)
        val currentMonth = (cal.get(Calendar.MONTH) + 1).toString
        val nextMonthOne = (cal.get(Calendar.MONTH) + 2).toString + "01"
        val endOfMonth = cal.getMaximum(Calendar.DAY_OF_MONTH).toString

        loadDateFormat = year + "{" + currentMonth + "*" + "," + nextMonthOne + "}"
        startDateFormat = year + "-" + currentMonth + "-" + "01"
        endDateForamt = year + "-" + currentMonth + "-" + endOfMonth

        //TODO 是否需要写到固定的常量类or通过SDK读取
        val inputPath=p.paramMap.getOrElse("inputPath","/log/moretvloginlog/parquet/#{date}/loginlog")
        val loadPathTemplate =inputPath.replace("#{date}",loadDateFormat)

        (loadPathTemplate, startDateFormat, endDateForamt)

      }
      case "week" => {
        null
      }
      case "quarter" => {
        null
      }
      case "day" => {
        null
      }
      case "year" => {
        null
      }

      case _ => null
    }
  }

}

