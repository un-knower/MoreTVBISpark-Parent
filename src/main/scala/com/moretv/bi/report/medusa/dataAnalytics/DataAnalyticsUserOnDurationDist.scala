package com.moretv.bi.report.medusa.dataAnalytics

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 3/11/17.
  */
object DataAnalyticsUserOnDurationDist extends BaseClass {


  private val tableName4DurationDist = "data_analytic_on_duration_distribution"

  private val tableName4TimesDist = "data_analytic_on_times_distribution"

  private val fields4Duration = Array("day", "version", "duration", "user_count", "count")

  private val fields4Times = Array("day", "version", "times", "user_count")

  private val insertSql4Duration = s"insert into $tableName4DurationDist(${fields4Duration.mkString(",")})values(?,?,?,?,?)"

  private val insertSql4Times = s"insert into $tableName4TimesDist(${fields4Times.mkString(",")})values(?,?,?,?)"

  private val deleteSql4Duration = s"delete from $tableName4DurationDist where day = ?"

  private val delteSql4Times = s"delete from $tableName4TimesDist where day = ?"

  def main(args: Array[String]) {


    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "80")

    ModuleClass.executor(this, args)
  }


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        try {

          val cal = Calendar.getInstance
          cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)


          (0 until p.numOfDays).foreach(d => {

            val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
            cal.add(Calendar.DAY_OF_YEAR, -1)
            val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

            println(sqlDate)
            if (p.deleteOld) {
              util.delete(deleteSql4Duration, sqlDate)
              util.delete(delteSql4Times, sqlDate)
            }

            val exit3 = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.EXIT, loadDate)

            val exit2 = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.EXIT, loadDate)

            val enter3 = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.ENTER, loadDate)

            val enter2 = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.ENTER, loadDate)

            userOnDurarionDist(exit2, exit3, sqlDate)
              .collect.foreach(
              r => {
                util.insert(insertSql4Duration, sqlDate, r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3))
              }
            )

            userOnTimesDist(enter2, enter3, sqlDate).collect.foreach(
              r => {
                util.insert(insertSql4Times, sqlDate, r.getString(0), r.getLong(1), r.getLong(2))
              }
            )

          })

        } catch {
          case e: Exception => {
            e.printStackTrace()
          }
        }


      }
      case None => {

      }
    }
  }


  def userOnDurarionDist(df2: DataFrame, df3: DataFrame, date: String): DataFrame = {

    val s = sqlContext

    import org.apache.spark.sql.functions._
    import s.implicits._

    val df = df2.select($"apkVersion", $"date", $"userId", $"duration".cast("long"))
      .unionAll(
        df3.select($"apkVersion", $"date", $"userId", $"duration")
      )

      .filter($"date" === date
        && substring($"apkVersion", 0, 1).isin("2" :: "3" :: Nil: _*)
      )

      .groupBy(substring($"apkVersion", 0, 1).as("version"), $"duration")
      .agg(countDistinct($"userId").as("user_count"), count($"userId").as("count"))

    df
  }

  def userOnTimesDist(df2: DataFrame, df3: DataFrame, date: String): DataFrame = {

    val sq = sqlContext

    import org.apache.spark.sql.functions._
    import sq.implicits._


    val df = df2.select($"apkVersion", $"userId", $"datetime", $"date")
      .unionAll(
        df3.select($"apkVersion", $"userId", $"datetime", $"date")
      )
      .filter($"date" === date
        && substring($"apkVersion", 0, 1).isin("2" :: "3" :: Nil: _*)
      )
      .groupBy(substring($"apkVersion", 0, 1).as("version"), $"userId")
      .agg(count($"datetime").as("count"))
      .groupBy($"version", $"count")
      .agg(count($"userId").as("user_count"))
    df

  }


  case class UserTimesDim {

    //    public
  }

}
