package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.udf.LauncherAccessAreaParser
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 统计medusa 3.0.6/motetv 2.6.7的点击情况
 */
object MedusaAndMoretvUnDailyUpdateClickInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)

        sqlContext.udf.register("launcherAccessAreaParser",LauncherAccessAreaParser.launcherAccessAreaParser _)
        sqlContext.udf.register("launcherLocationIndexParser",LauncherAccessAreaParser.launcherLocationIndexParser _)
        sqlContext.udf.register("moretvLauncherAccessLocationParser",LauncherAccessAreaParser.moretvLauncherAccessLocationParser _)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
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

          medusaHomeaccesslog.select("userId","accessArea","apkVersion","locationIndex","event","buildDate","accessLocation")
            .registerTempTable("medusa_homeaccess_log")

          moretvHomeaccesslog.select("userId","accessArea","apkVersion","accessLocation","event")
            .registerTempTable("moretv_homeaccess_log")

          sqlContext.read.load(s"/log/medusa/mac2UserId/$date/medusaDailyUnUpdateActiveUserId")
            .registerTempTable("medusa_un_daily_update_log")
          sqlContext.read.load(s"/log/medusa/mac2UserId/$date/moretvDailyUnUpdateActiveUserId")
            .registerTempTable("moretv_un_daily_update_log")




          /*medusa 3.0.6 今日推荐点击*/
          val sqlSumSparkMedusa = "" +
            "select a.apkVersion,launcherAccessAreaParser(a.accessArea,'medusa'),count(a.userId),count" +
            "(distinct a.userId) " +
            "from medusa_homeaccess_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a.userId not " +
            "like '999999999999%' and a.apkVersion='3.0.6' and a" +
            ".accessArea='recommendation' and a.event='click' " +
            "group by a.apkVersion,launcherAccessAreaParser(a.accessArea,'medusa')"
          val medusaRecommendRddSummary = sqlContext.sql(sqlSumSparkMedusa).map(e=>(e.getString(0),e.getString(1),e.getLong
            (2),e.getLong(3))).collect()
          /*medusa 3.0.6 直播点击*/
          val medusaLiveClickRdd=sqlContext.sql("select a.apkVersion,count(a.userId),count(distinct a.userId) from " +
            "medusa_homeaccess_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a" +
            ".accessArea='live' and" +
            " a.apkVersion='3.0.6' and a.userId not like '9999999999%' group by a.apkVersion").map(e=>(e.getString(0),e
            .getLong(1),e.getLong(2))).collect()
          /*medusa 3.0.6 分类频道点击*/
          val medusaClassificationRdd=sqlContext.sql("select a.apkVersion,count(a.userId) as total_num,count(distinct a" +
            ".userId) as total_user from medusa_homeaccess_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a.userId not like '99999999%' " +
            "and a.event='click' and a.apkVersion in ('3.0.6') and a.accessArea in ('my_tv','classification') and a" +
            ".accessLocation not in ('history','collect') group by a.apkVersion").map(e=>(e.getString(0),e.getLong(1),e
            .getLong(2))).collect()
          /*medusa 3.0.6 历史收藏点击*/
          val medusaHistoryAndCollectRdd=sqlContext.sql("select a.apkVersion,count(a.userId) as total_num,count" +
            "(distinct a.userId) as total_user from medusa_homeaccess_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a.userId not like " +
            "'99999999%' and a.event='click' and a.apkVersion in ('3.0.6') and a.accessArea ='my_tv' and a.accessLocation " +
            "in ('history','collect') group by a.apkVersion").map(e=>(e.getString(0),e.getLong(1),e.getLong(2))).collect()



          /*moretv 2.6.7 热门推荐点击*/
          val sqlSumSparkMoretv = "" +
            "select apkVersion,launcherAccessAreaParser(accessArea,'moretv'),count(a.userId),count(distinct a.userId) " +
            "from moretv_homeaccess_log as a join moretv_un_daily_update_log as b on a.userId=b.userId where a.userId not " +
            "like '99999999999%' and accessArea='1' and apkVersion in ('2.6.7') and event='enter'" +
            "group by apkVersion,launcherAccessAreaParser(accessArea,'moretv')"
          val moretvRecommendRddSummary = sqlContext.sql(sqlSumSparkMoretv).map(e=>(e.getString(0),e.getString(1),e
            .getLong
            (2),e.getLong(3))).collect()

          /*moretv 2.6.7 直播点击*/
           val moretvLiveClickRdd=sqlContext.sql("select a.apkVersion,count(a.userId), count(distinct a.userId) " +
            "from moretv_homeaccess_log as a join moretv_un_daily_update_log as b on a.userId=b.userId where a.accessArea='5' and a.accessLocation='4' and a.apkVersion='2.6.7' " +
            "and a.userId not like '99999999999%' group by a.apkVersion").map(e=>(e.getString(0),e.getLong(1),e.getLong(2)
            )).collect()

          /*moretv 2.6.7 分类频道点击*/
          val moretvClassificationClickRdd=sqlContext.sql("select a.apkVersion,count(a.userId) as total_num,count(distinct" +
            " a.userId) as total_user from moretv_homeaccess_log as a join moretv_un_daily_update_log as b on a.userId=b.userId where a.userId not like " +
            "'99999999%' and a.apkVersion='2.6.7' and a.event='enter' and a.accessArea='5' and " +
            "a.accessLocation in ('2','3','5','6','8','9','10','11','12','13','14') group by a.apkVersion").map(e=>(e.getString(0),e.getLong(1),e.getLong(2)
            )).collect()

          /*moretv 2.6.7 历史收藏点击*/
          val moretvHistoryAndCollectClickRdd=sqlContext.sql("select a.apkVersion,count(a.userId) as total_num,count" +
            "(distinct a.userId) as total_user from moretv_homeaccess_log as a join moretv_un_daily_update_log as b on a.userId=b.userId where a.userId not " +
            "like '99999999%' and a.apkVersion='2.6.7' and a.event='enter' and ((a.accessArea='5' and a.accessLocation " +
            "='1')) group by a.apkVersion").map(e=>(e.getString(0),e.getLong(1),e.getLong(2))).collect()

          val sqlInsert = "insert into medusa_mtv_gray_testing_click_info(day,apk_version," +
            "accessAreaName,click_num,click_user) values (?,?,?,?,?)"
          medusaRecommendRddSummary.foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,i._2,new JLong(i._3),new JLong(i._4))
          })
          medusaClassificationRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1,"分类频道",new JLong(e._2),new JLong(e._3))
          })
          medusaHistoryAndCollectRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1,"历史收藏",new JLong(e._2),new JLong(e._3))
          })
          medusaLiveClickRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1,"直播",new JLong(e._2),new JLong(e._3))
          })

          moretvRecommendRddSummary.foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,i._2,new JLong(i._3),new JLong(i._4))
          })
          moretvClassificationClickRdd.foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,"分类频道",new JLong(i._2),new JLong(i._3))
          })
          moretvHistoryAndCollectClickRdd.foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,"历史收藏",new JLong(i._2),new JLong(i._3))
          })
          moretvLiveClickRdd.foreach(i=>{
            util.insert(sqlInsert,insertDate,i._1,"直播",new JLong(i._2),new JLong(i._3))
          })




        })






      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
