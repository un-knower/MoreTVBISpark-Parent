package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.udf.LauncherAccessAreaParser
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object UnDailyUpdateMedusaAndMoretvRecommendClickInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)

        sqlContext.udf.register("launcherAccessAreaParser",LauncherAccessAreaParser.launcherAccessAreaParser _)
        sqlContext.udf.register("launcherLocationIndexParser",LauncherAccessAreaParser.launcherLocationIndexParser _)
        sqlContext.udf.register("moretvLauncherAccessLocationParser",LauncherAccessAreaParser.moretvLauncherAccessLocationParser _)
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val numOfDays = p.numOfDays
        val insertDate = DateFormatUtils.toDateCN(startDate,-1)

        val medusaDir = "/log/medusa/parquet"
        val moretvDir = "/mbi/parquet"
        val logType = "homeaccess"

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaHomeaccessInput = s"$medusaDir/$date/$logType/"
          val moretvHomeaccessInput = s"$moretvDir/$logType/$date/"

          val medusaHomeaccesslog = sqlContext.read.parquet(medusaHomeaccessInput)
          val moretvHomeaccesslog = sqlContext.read.parquet(moretvHomeaccessInput)

          medusaHomeaccesslog.select("userId","accessArea","apkVersion","locationIndex","event","buildDate")
            .registerTempTable("medusa_homeaccess_log")
          moretvHomeaccesslog.select("userId","accessArea","apkVersion","accessLocation","event")
            .registerTempTable("moretv_homeaccess_log")
          sqlContext.read.load(s"/log/medusa/mac2UserId/$date/medusaDailyUnUpdateActiveUserId")
            .registerTempTable("medusa_un_daily_update_log")
          sqlContext.read.load(s"/log/medusa/mac2UserId/$date/moretvDailyUnUpdateActiveUserId")
            .registerTempTable("moretv_un_daily_update_log")



          val sqlSumSparkMedusa = "" +
            "select a.apkVersion,launcherAccessAreaParser(a.accessArea,'medusa'),count(a.userId),count" +
            "(distinct a.userId) " +
            "from medusa_homeaccess_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a.userId not " +
            "like '999999999999%' and a.apkVersion='3.0.6' and a" +
            ".accessArea='recommendation' and a.event='click' " +
            "group by a.apkVersion,launcherAccessAreaParser(a.accessArea,'medusa')"
          val sqlDifferentLocationSparkMedusa = "" +
            "select a.apkVersion,launcherAccessAreaParser(a.accessArea,'medusa'),launcherLocationIndexParser" +
            "(a.locationIndex),count(a.userId), count(distinct a.userId) " +
            "from medusa_homeaccess_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a.userId not " +
            "like '999999999999%' and a.accessArea='recommendation' and a.apkVersion='3.0.6' and a.event='click' " +
            "group by a.apkVersion,launcherAccessAreaParser(a.accessArea,'medusa'),launcherLocationIndexParser" +
            "(a.locationIndex)"

          val medusaRecommendRdd = sqlContext.sql(sqlDifferentLocationSparkMedusa).map(e=>(e.getString(0),e.getString(1),e
            .getString(2),e.getLong(3),e.getLong(4)))
          val medusaRecommendRddSummary = sqlContext.sql(sqlSumSparkMedusa).map(e=>(e.getString(0),e.getString(1),e.getLong
            (2),e.getLong(3)))
          val sqlInsert = "insert into medusa_gray_testing_recommend_undailyupdate_each_day(date,apkVersion," +
            "accessAreaName,locationIndexName,click_num,click_user) values (?,?,?,?,?,?)"
          medusaRecommendRdd.collect().foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,i._2,i._3,new JLong(i._4),new JLong(i._5))
          })
          medusaRecommendRddSummary.collect().foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,i._2,"All",new JLong(i._3),new JLong(i._4))
          })



          val sqlDifferentLocationSparkMoretv = "" +
            "select a.apkVersion,launcherAccessAreaParser(a.accessArea,'moretv')," +
            "moretvLauncherAccessLocationParser(a.accessArea,a.accessLocation),count(a.userId), count(distinct a.userId) " +
            "from moretv_homeaccess_log as a join moretv_un_daily_update_log as b on a.userId=b.userId " +
            "where a.userId not like '999999999999%' and accessArea='1' and apkVersion='2.6.7' and event='enter' " +
            "group by apkVersion,moretvLauncherAccessLocationParser(accessArea,accessLocation)," +
            "launcherAccessAreaParser(accessArea,'moretv')"
          val sqlSumSparkMoretv = "" +
            "select apkVersion,launcherAccessAreaParser(accessArea,'moretv'),count(a.userId),count(distinct a.userId) " +
            "from moretv_homeaccess_log as a join moretv_un_daily_update_log as b on a.userId=b.userId where a.userId not " +
            "like '99999999999%' and accessArea='1' and apkVersion='2.6.7' and event='enter'" +
            "group by apkVersion,launcherAccessAreaParser(accessArea,'moretv')"

          val moretevRecommendRdd = sqlContext.sql(sqlDifferentLocationSparkMoretv).map(e=>(e.getString(0),e.getString(1),e
            .getString(2),e.getLong(3),e.getLong(4)))
          val moretvRecommendRddSummary = sqlContext.sql(sqlSumSparkMoretv).map(e=>(e.getString(0),e.getString(1),e.getLong
            (2),e.getLong(3)))

          moretevRecommendRdd.collect().foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,i._2,i._3,new JLong(i._4),new JLong(i._5))
          })
          moretvRecommendRddSummary.collect().foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,i._2,"All",new JLong(i._3),new JLong(i._4))
          })
        })






      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
