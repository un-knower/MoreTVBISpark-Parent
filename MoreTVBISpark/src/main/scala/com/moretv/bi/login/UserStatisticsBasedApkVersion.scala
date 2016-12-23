package com.moretv.bi.login

import java.util.Calendar

import org.apache.spark.sql.functions._
import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
  * Created by zhangyu and xiajun on 2016/7/20.
  * 统计分版本的日新增、活跃及累计用户数，采用mac去重方式。
  * tablename: medusa.medusa_user_statistics_based_apkversion
  * (id,day,apk_version,adduser_num,accumulate_num,active_num)
  * Params : startDate, numOfDays(default = 1),deleteOld;
  *
  */
object UserStatisticsBasedApkVersion extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(UserStatisticsBasedApkVersion, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        //cal1代表用户数据库快照对应日期，cal2代表活跃用户库日志对应日期
        val cal1 = Calendar.getInstance()
        cal1.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal1.add(Calendar.DAY_OF_MONTH, -1)

        val cal2 = Calendar.getInstance()
        cal2.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val addLogdate = DateFormatUtils.readFormat.format(cal1.getTime)
          val addTimeday = DateFormatUtils.toDateCN(addLogdate)
          val startTime = s"$addTimeday" + " " + "00:00:00"
          val endTime = s"$addTimeday" + " " + "23:59:59"

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MTVACCOUNT, addLogdate)
            .filter(s"openTime  <= $endTime")
            .select("current_version", "openTime", "mac")
            .registerTempTable("log_dat")

          val dfd = sqlContext.sql(
            """
              | select case
              |   when current_version is null then 'null'
              |   when current_version = '' then 'kong'
              |   else current_version as version, mac
              | from log_dat
            """.stripMargin)

          sqlContext.dropTempTable("log_dat")

          val addMap =
            dfd.filter(s"openTime between '$startTime' and '$endTime'")
              .groupBy("version")
              .agg(count("mac").alias("counts"))
              .collectAsList
              .toMap

          val accumulateMap =
            dfd.filter(s"openTime < = '$endTime'")
              .groupBy("version")
              .agg(count("mac").alias("counts"))
              .collectAsList
              .toMap

          val activeLogdate = DateFormatUtils.readFormat.format(cal2.getTime)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, activeLogdate)
            .select("mac", "version")
            .registerTempTable("activelog_data")

          val activeMap = sqlContext.sql(
            s"""
               |select case
               |   when version is null then 'null'
               |   when version = '' then 'kong'
               |   else version as version, mac
               | from log_dat
             """.stripMargin)
            .groupBy("version")
            .agg(count("mac").alias("counts"))
            .collectAsList.toMap

          if (p.deleteOld) {
            val deleteSql = "delete from medusa_user_statistics_based_apkversion where day = ?"
            util.delete(deleteSql, addTimeday)
          }

          val insertSql = "insert into medusa_user_statistics_based_apkversion(day,apk_version,adduser_num,accumulate_num,active_num) values(?,?,?,?,?)"

          val keys = addMap.keySet.union(accumulateMap.keySet).union(activeMap.keySet)

          keys.foreach(key => {
            val adduser_num = addMap.getOrElse(key, 0)
            val accumulate_num = accumulateMap.getOrElse(key, 0)
            val active_num = activeMap.getOrElse(key, 0)
            util.insert(insertSql, addTimeday, key, adduser_num, accumulate_num, active_num)
          }

          )
          cal1.add(Calendar.DAY_OF_MONTH, -1)
          cal2.add(Calendar.DAY_OF_MONTH, -1)
        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }
}
