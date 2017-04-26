package com.moretv.bi.login

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by zhangyu on 2016/7/20.
  * 统计分版本的日新增、活跃及累计用户数，采用mac去重方式。
  * tablename: medusa.medusa_user_statistics_based_apkversion
  * (id,day,apk_version,adduser_num,accumulate_num,active_num)
  * Params : startDate, numOfDays(default = 1),deleteOld;
  *
  */
object UserStatisticsBasedApkVersion extends BaseClass {

  private val cnFormat = new SimpleDateFormat("yyyy-MM-dd")

  private val readFormat = new SimpleDateFormat("yyyyMMdd")

  private val tableName = "medusa_user_statistics_based_apkversion"

  private val insertSql = s"insert into $tableName(day,apk_version,adduser_num,accumulate_num,active_num) values(?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val s = sqlContext
        import s.implicits._

        val versionUdf = udf((s: String) => {
          s match {
            case "" => "kong"
            case _ => if (s == null) {
              "null"
            } else {
              s
            }
          }
        })
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance

        (0 until p.numOfDays).foreach(i => {

          cal.setTime(readFormat.parse(p.startDate))
          val loadDate = readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -1)
          val loadDate1 = readFormat.format(cal.getTime)
          val sqlDate = cnFormat.format(cal.getTime)


          val dfd1 = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, loadDate1)
            .select(
              versionUdf($"current_version").as("version"),
              to_date($"openTime").as("date"),
              $"mac"
            )

          val dfd2 = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, loadDate)
            .select($"mac", versionUdf($"version").as("version")).distinct()
            .groupBy("version")
            .agg(countDistinct("mac").alias("counts"))

          val resDf = dfd1.filter($"date" === sqlDate)
            .groupBy("version")
            .agg(countDistinct("mac").alias("counts"))
            .as("t1")
            .join(
              dfd1.filter($"date" <= sqlDate)
                .groupBy("version")
                .agg(countDistinct("mac").alias("counts"))
                .as("t2"),
              $"t1.version" === $"t2.version")
            .join(
              dfd2.as("t3"), $"t2.version" === $"t3.version")
            .select($"t1.version", $"t1.counts", $"t2.counts", $"t3.counts")

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          resDf.collect.foreach(e => {
           // println(sqlDate, e.getString(0), e.getLong(1), e.getLong(2), e.getLong(3))
            util.insert(insertSql, sqlDate, e.getString(0), e.getLong(1), e.getLong(2), e.getLong(3))
          }
          )

          util.destory


        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }

}
