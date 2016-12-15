package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object MedusaAndMoretvDailyActiveUserInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
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

          val medusaDailyActiveUserRdd=sqlContext.sql("select apkVersion,count(distinct userId) from medusa_daily_active_log " +
            " where apkVersion in ('3.0.6') group by apkVersion").map(e=>(e.getString(0),e.getLong(1))).map(e=>(e._1,e._2))


          val moretvDailyActiveUserRdd = sqlContext.sql("select apkVersion,count(distinct userId) from " +
            "moretv_daily_active_log where apkVersion in ('2.6.7') group by apkVersion").map(e=>(e.getString(0),e.getLong(1))).map(e=>(e._1,e._2))


          val sqlInsert = "insert into medusa_mtv_gray_testing_each_apk_daily_active_user_from_all_log(day,apk_version," +
            "active_user,flag) values " +
            "(?,?,?,?)"

          medusaDailyActiveUserRdd.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1,new JLong(e._2),"all")
          })
          moretvDailyActiveUserRdd.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1,new JLong(e._2),"all")
          })
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
