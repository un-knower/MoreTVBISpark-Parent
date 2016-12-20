package com.moretv.bi.report.medusa.userDevelop

import java.text.SimpleDateFormat
import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.Params
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.DataFrame

/**
 * Created by witnes on 8/29/16.
 * 统计电视猫 2.\* & 3.\* 版本下 月活 周活 日活
 *
 */
object activeUserCountInfo extends BaseClass {

  private val monthCountTable = "active_user_count_month"

  private val weekCountTable = "active_user_count_week"

  private val dayCountTable = "active_user_count_day"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(activeUserCountInfo, args)
  }


  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val today = Calendar.getInstance()
        today.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        /** 当月第一天 => 月活 **/
        today.get(Calendar.DAY_OF_MONTH) match {
          case 1 => {
            val todayCopy = Calendar.getInstance
            todayCopy.setTime(today.getTime)
            todayCopy.add(Calendar.DATE, -1)
            val numOfDays = todayCopy.get(Calendar.DAY_OF_MONTH)
            val uv = constructDF(numOfDays, today, p).distinct.count
            dbOperation(uv, monthCountTable, "month", today, p.deleteOld)
          }
          case _ => null
        }
        // 当周第一天 => 周活 | Sunday => 1 ,Monday => 2
        today.get(Calendar.DAY_OF_WEEK) match {
          case 2 => {
            val numOfDays = 7
            val uv = constructDF(numOfDays, today, p).distinct.count
            dbOperation(uv, weekCountTable, "weekstart_end", today, p.deleteOld)
          }
          case _ => null
        }
        /* 算日活 */
        val numOfDays = 1
        val uv = constructDF(numOfDays, today, p).distinct.count
        dbOperation(uv, dayCountTable, "day", today, p.deleteOld)
      }
      case None =>
        throw new RuntimeException("at least needs one param: startDate")
    }
  }


  /**
   *
   * @param diffDays 统计计算的天数 (月活:一个月的天数, 周活: 7天, 日活 :1天)
   * @param cal      执行命令的当天(需减一天开始计算)
   * @return DataFrame格式
   */
  def constructDF(diffDays: Int, cal: Calendar, p: Params): DataFrame = {
    val calCopy = Calendar.getInstance
    calCopy.setTime(cal.getTime)

    val medusaPath = new Array[String](diffDays)
    val mbiPath = new Array[String](diffDays)
    var date = DateFormatUtils.readFormat.format(calCopy.getTime)

    for (i <- 0 until diffDays) {
      date = DateFormatUtils.readFormat.format(calCopy.getTime)
      medusaPath(i) = DataIO.getDataFrameOps.getPath(MEDUSA, "*", date)
      mbiPath(i) = DataIO.getDataFrameOps.getPath(MORETV, "*", date)
      println(medusaPath(i))
      println(mbiPath(i))
      calCopy.add(Calendar.DATE, -1)
    }

    val medusaDf = sqlContext.read.parquet(medusaPath: _*).select("userId")
    val mbiDf = sqlContext.read.parquet(mbiPath: _*).select("userId").filter("userId!= ''")
    val mergeDf = medusaDf.unionAll(mbiDf)

    mergeDf
  }


  /**
   *
   * @param uv        日活|月活|周活
   * @param table     active_user_count_month|active_user_count_week|active_user_count_day 表名
   * @param field     day|month|weekstart_end 字段
   * @param cal       执行命令的当天(需减一天插入表格)
   * @param deleteOld 是否删除旧数据
   */
  def dbOperation(uv: Long, table: String, field: String, cal: Calendar, deleteOld: Boolean): Unit = {

    val calCopy = Calendar.getInstance
    calCopy.setTime(cal.getTime)
    calCopy.add(Calendar.DATE, -1)
    println(calCopy.getTime)

    val insertDate =
      field match {
        case "day" => new SimpleDateFormat("yyyy-MM-dd").format(calCopy.getTime)
        case "month" => new SimpleDateFormat("MM月").format(calCopy.getTime)
        case "weekstart_end" => {
          val end = new SimpleDateFormat("yyyy-MM-dd").format(calCopy.getTime)
          calCopy.add(Calendar.DATE, -6)
          val start = new SimpleDateFormat("yyyy-MM-dd~").format(calCopy.getTime)
          start + end
        }
      }
    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

    val deleteSql = s"delete from $table where $field=?"
    val insertSql = s"insert into $table ($field,user_num) values(?,?)"

    if (deleteOld) {
      util.delete(deleteSql, insertDate)
    }
    try {
      println(table, field, insertDate, uv)
      util.insert(insertSql, insertDate, new JLong(uv))
    } catch {
      case ex: Exception => println(ex)
    }

  }
}
