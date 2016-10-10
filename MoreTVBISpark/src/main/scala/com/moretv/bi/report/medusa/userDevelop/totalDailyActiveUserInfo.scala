package com.moretv.bi.report.medusa.userDevelop

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object totalDailyActiveUserInfo extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(totalDailyActiveUserInfo,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val medusaDir = "/log/medusa/parquet"
        val moretvDir = "/mbi/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaDailyActiveInput = s"$medusaDir/$date/*/"
          val moretvDailyActiveInput = s"$moretvDir/*/$date"

          val medusaDailyActivelog = sqlContext.read.parquet(medusaDailyActiveInput).select("userId","apkVersion")
            .registerTempTable("medusa_daily_active_log")
          val moretvDailyActivelog = sqlContext.read.parquet(moretvDailyActiveInput).select("userId","apkVersion")
            .registerTempTable("moretv_daily_active_log")

          val totalActiveUser=sqlContext.sql("select count(distinct a.userId) from (select distinct userId from " +
            "medusa_daily_active_log Union select distinct userId from moretv_daily_active_log) as a").map(e=>e.getLong(0))

          val sqlInsert = "insert into medusa_user_develop_total_active_user_info(day,active_user) values " +
            "(?,?)"

          totalActiveUser.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,new JLong(e))
          })

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
