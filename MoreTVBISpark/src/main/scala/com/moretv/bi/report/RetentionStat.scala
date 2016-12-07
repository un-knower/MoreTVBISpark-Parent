package com.moretv.bi.report

import java.util.Calendar
import java.lang.{Float => JFloat, Integer => JInt, Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by witnes on 11/14/16.
  */

/**
  * 用户留存率统计
  * 1天， 2天， ... 7天, 14天， 30天內
  */
object RetentionStat extends BaseClass {

  import org.apache.spark.sql.types.StructField

  private val tableName = "retention_day_step_stat"

  private val insertSql = s"insert into $tableName(day,duration,retention_rate,user_num) values(?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"


  def main(args: Array[String]) {
    ModuleClass.executor(RetentionStat, args)
  }


  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        val calBase = Calendar.getInstance
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        calBase.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(w => {
          calBase.add(Calendar.DAY_OF_YEAR, -1)
          cal.setTime(calBase.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -1)
          val endDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -30)
          val startDate = DateFormatUtils.cnFormat.format(cal.getTime)
          val loadNewAddedPath = s"/log/dbsnapshot/parquet/$endDate/moretv_mtv_account"

          val dfBase = sqlContext.read.parquet(loadNewAddedPath)
            .filter(s"openTime between '$startDate 00:00:00' and '$startDate 23:59:59'")
            .select("mac")
            .distinct

          val days = Array(1, 2, 3, 4, 5, 6, 7, 14, 30)
          val subDays = Array(1, 1, 1, 1, 1, 1, 1, 7, 16)

          val loadDatePaths = new ArrayBuffer[Array[String]]
          val datesStartEnd = new ArrayBuffer[(String, String)]

          val loadPaths = new ArrayBuffer[String]

          cal.add(Calendar.DAY_OF_YEAR, 1) //留存率开始日期
          val start = DateFormatUtils.cnFormat.format(cal.getTime)

          subDays.zipWithIndex.foreach {

            case (day, index) => {
              var end = ""
              (1 to day).foreach(days => {
                end = DateFormatUtils.cnFormat.format(cal.getTime)
                cal.add(Calendar.DAY_OF_YEAR, 1) //loadPath : T+1
                val nextDay = DateFormatUtils.readFormat.format(cal.getTime)
                val nextPath = s"/log/moretvloginlog/parquet/$nextDay/loginlog"
                loadPaths.+=(nextPath)
              })

              loadDatePaths.+=(loadPaths.toArray)
              datesStartEnd.+=((start, end))
            }
          }

          val retentionRate = new ArrayBuffer[(Int, Float)]

          val baseNum = dfBase.count
          loadDatePaths.toArray.zipWithIndex foreach { case (path, index) => {

            val ratio = sqlContext.read.parquet(path: _*)
              .filter(s"datetime between '${datesStartEnd(index)._1} 00:00:00' and '${datesStartEnd(index)._2} 23:59:59'")
              .select("mac").distinct
              .intersect(dfBase)
              .count / baseNum.toFloat

            retentionRate.+=((days(index), ratio))

          }
          }

          util.delete(deleteSql, startDate)

          retentionRate.toArray.foreach(println)
          retentionRate.toArray.foreach(res => {
            val index = res._1
            util.insert(insertSql, startDate, new JInt(index), new JFloat(res._2), new JLong(baseNum))
          })
        })


      }
      case None => {

      }
    }
  }


}
