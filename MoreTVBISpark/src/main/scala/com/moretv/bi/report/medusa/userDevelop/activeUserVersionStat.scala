package com.moretv.bi.report.medusa.userDevelop

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 11/6/16.
  */

/**
  * 活跃用户计算
  *
  * 周活：前七天
  *
  */
object activeUserVersionStat extends BaseClass {

  private val tableName = "active_user_version_stat"

  private val fields = "day, version, uv"

  private val insertSql = s"insert $tableName($fields)values(?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ? "

  def main(args: Array[String]) {
    ModuleClass.executor(activeUserVersionStat, args)
  }


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        // init & util
        val util = new DBOperationUtils("medusa")

        val startDate = p.startDate

        val cal = Calendar.getInstance

        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path1
          val loadPath1 = s"/log/medusa/parquet/$loadDate/*"

          val loadPath2 = s"/mbi/parquet/*/$loadDate"

          val loads = new Array[String](2)

          loads(0) = loadPath1
          loads(1) = loadPath2

          sqlContext.read.parquet(loads: _*)
            .filter(s"date = '$sqlDate'")
            .select("userId", "apkVersion")
            .distinct
            .registerTempTable("log_data")

          val df = sqlContext.sql(
            """
              |select substr(apkVersion,1,1) as version, count(distinct userId) as uv
              |from log_data
              |group by substr(apkVersion,1,1)
            """.stripMargin)

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          df.collect.foreach(w => {
            val version = w.getString(0)
            val uv = new JLong(w.getLong(1))
            util.insert(insertSql, sqlDate, version, uv)
          })

        })

      }
      case None => {}

    }

  }
}
