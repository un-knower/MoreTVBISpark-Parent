package com.moretv.bi.report.medusa

import java.lang.{Double => JDouble, Integer => JInt, Long => JLong}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * Created by witnes on 11/14/16.
  */

/**
  * 用户留存率统计
  * 1天， 2天， ... 7天, 14天， 30天內
  * 维度 ：日期 ，周期
  * 昨天起 - 日活
  * 前天起 - 新增用户数
  **/
object RetentionStepStat extends BaseClass {

  private val tableName = "retention_day_stat"

  private val insertSql = s"insert into $tableName(day,duration,retention_rate,user_num) values(?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ? and duration = ? "

  private val days = Array(1, 2, 3, 4, 5, 6, 7, 14, 30) // for sql Date

  private val subDays = Array(1, 1, 1, 1, 1, 1, 1, 7, 16) // for loadPath

  private val readFormat = new SimpleDateFormat("yyyyMMdd")

  private val sqlFormat = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]) {
    ModuleClass.executor(RetentionStepStat, args)
  }


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(w => {

          val ref = getSomeNewUser(cal) // 新增用户

          val loadDatePaths = getLoadPaths(cal) // 加载路径

          val retentionRate = new ArrayBuffer[(Int, Double, Long)]

          //遍历留存率
          loadDatePaths.toArray.zipWithIndex foreach {
            case (path, index) => {
              val activeCounts = getSomeActiveUser(path, ref._1, days(index), cal)
              val base = ref._2.get(days(index)) match {
                case Some(p) => p
                case None => 1
              }
              val ratio = activeCounts / base.toDouble
              retentionRate.+=((days(index), ratio, base))
            }
          }

          save2Db(cal, retentionRate.toArray)

          cal.add(Calendar.DAY_OF_YEAR, -1)
        })


      }
      case None => {

      }
    }
  }



  def save2Db(cal: Calendar, resArray: Array[(Int, Double, Long)]) = {

    resArray.foreach(println)

    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

    val calCopy = Calendar.getInstance
    calCopy.setTime(cal.getTime)
    calCopy.add(Calendar.DAY_OF_YEAR, -1)

    resArray.foreach(res => {
      calCopy.add(Calendar.DAY_OF_YEAR, -subDays(days.indexOf(res._1)))
      val curDate = sqlFormat.format(calCopy.getTime)
      util.delete(deleteSql, curDate, new JInt(res._1))
      util.insert(insertSql, curDate, new JInt(res._1), new JDouble(res._2), new JLong(res._3))
    })

  }

  /**
    * 拿到数据源路径
    * @param cal 基准日期, 也是当前日期
    * @return 与基准日期相差指定时段（days）的路径数组
    */
  def getLoadPaths(cal: Calendar): ArrayBuffer[Array[String]] = {

    val calCopy = Calendar.getInstance
    calCopy.setTime(cal.getTime)

    val loadDatePaths = new ArrayBuffer[Array[String]]
    val loadPaths = new ArrayBuffer[String]

    subDays.zipWithIndex.foreach {

      case (day, index) => {

        (1 to day).foreach(days => {

          val nextDay = readFormat.format(calCopy.getTime)
          val nextPath = s"/log/moretvloginlog/parquet/$nextDay/loginlog"
          loadPaths.+=(nextPath)
          calCopy.add(Calendar.DAY_OF_YEAR, -1) //loadPath : T+1

        })

        loadDatePaths.+=(loadPaths.toArray)
      }
    }
    loadDatePaths
  }

  /**
    * 取新增用户的时间早活跃用户一天
    * 得到不同天数前的新增用户数 (前天开始, 相较于昨天的天数)
    *
    * @param cal
    * @return
    */
  def getSomeNewUser(cal: Calendar): (DataFrame, scala.collection.Map[Int, Long]) = {

    val calCopy = Calendar.getInstance
    calCopy.setTime(cal.getTime)
    calCopy.add(Calendar.DAY_OF_YEAR, -1)

    val loadDate = readFormat.format(calCopy.getTime)
    val sqlDate = sqlFormat.format(calCopy.getTime) //昨天日期
    val loadPath = s"/log/dbsnapshot/parquet/$loadDate/moretv_mtv_account"

    val dfRaw = sqlContext.read.parquet(loadPath)
      .selectExpr(s"datediff('$sqlDate',openTime) as duration", "mac")
      .filter(s"duration in (${days.mkString(",")})")
      .distinct

    dfRaw.registerTempTable("log_data")

    val dateCountMap = sqlContext.sql(
      """
        |select duration,count(distinct mac)
        |from log_data
        |group by duration
      """.stripMargin)
      .map(e => (e.getInt(0), e.getLong(1)))
      .collectAsMap

    (dfRaw, dateCountMap)

  }

  /**
    *
    * @param loadPath  加载数据路径集
    * @param dfRef     做交集df
    * @param numOfDays 向前的天数
    * @param cal       基点日期
    * @return
    */
  def getSomeActiveUser(loadPath: Array[String], dfRef: DataFrame, numOfDays: Int, cal: Calendar): Long = {

    val calCopy = Calendar.getInstance
    calCopy.setTime(cal.getTime)
    calCopy.add(Calendar.DAY_OF_YEAR, -1)
    val sqlDate = sqlFormat.format(calCopy.getTime)

    sqlContext.read.parquet(loadPath: _*)
      .filter(s"datediff('$sqlDate',date) between 0 and $numOfDays")
      .select("mac")
      .distinct
      .join(dfRef.filter(s"duration =$numOfDays"), "mac")
      .count

  }
}
