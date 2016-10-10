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
object MedusaAndMoretvUnDailyUpdatePlayInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate


        val medusaDir = "/log/medusa/parquet"
        val moretvDir = "/mbi/parquet"
        val medusaAndMoretvDir = "/log/temp/medusaAndMoretvMerger/parquet"
        val medusaPlayLogType = "playview"
        val moretvPlayLogType = "playview"
        val live="live"

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaPlayInput = s"$medusaDir/$date/$medusaPlayLogType/"
          val moretvPlayInput = s"$moretvDir/$moretvPlayLogType/$date/"


          val medusaAndMoretvInput = s"$medusaAndMoretvDir/$date/$moretvPlayLogType"

          val medusaPlaylog = sqlContext.read.parquet(medusaPlayInput)
          val moretvPlaylog = sqlContext.read.parquet(moretvPlayInput)
          val medusaAndMoretvLog = sqlContext.read.parquet(medusaAndMoretvInput)

          medusaPlaylog.select("userId","duration","apkVersion","event","contentType","buildDate")
            .registerTempTable("medusa_play_log")
          medusaAndMoretvLog.select("userId","duration","apkVersion","buildDate","launcherAreaFromPath","event",
            "launcherAccessLocationFromPath").registerTempTable("medusa_moretv_play_log")
          sqlContext.read.load(s"/log/medusa/mac2UserId/$date/medusaDailyUnUpdateActiveUserId")
            .registerTempTable("medusa_un_daily_update_log")
          sqlContext.read.parquet(s"$medusaDir/$date/$live").registerTempTable("medusa_live_log")

          /*medusa 3.0.6 整体播放情况*/
          val medusaPlayUserRdd = sqlContext.sql(
            "select a.apkVersion,count(a.userId),count(distinct a.userId),sum(a.duration)" +
              " from medusa_play_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a.userId not " +
              "like '99999999999999%' and a.apkVersion in ('3.0.6') and a.duration<=10800 and a.duration>=0 and a" +
              ".event='playview' group by a.apkVersion")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).collect()


          /*medusa 3.0.6 今日推荐播放情况*/
          val medusaRecommendlayUserRdd = sqlContext.sql(
            "select a.apkVersion,a.launcherAreaFromPath,count(a.userId),count(distinct a.userId),sum(a.duration)" +
              " from medusa_moretv_play_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a.userId " +
              "not like '99999999999999%' and a.apkVersion in ('3.0.6') and a.duration<=10800 and a" +
              ".duration>=0 and a.event ='playview' and a.launcherAreaFromPath in ('recommendation') group by a" +
              ".apkVersion,a.launcherAreaFromPath")
            .map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3),e
            .getLong(4))).collect()

          /*medusa 3.0.6 分类播放情况*/
          val medusaClassificationPlayUserRdd = sqlContext.sql(
            "select a.apkVersion,'classificationAndMytv',count(a.userId),count(distinct a.userId),sum(a" +
              ".duration) from medusa_moretv_play_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where" +
              " a.userId not like '99999999999999%' and a.apkVersion='3.0.6' and a.duration<=10800 and a" +
              ".duration>=0 and a.event ='playview' and a.launcherAreaFromPath in ('classification','my_tv') " +
              "and a.launcherAccessLocationFromPath not in ('account','history','collect') group by a.apkVersion")
            .map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3),e
            .getLong(4))).collect()

          /*medusa 3.0.6历史收藏播放情况*/
          val medusaHistoryAndCollectPlayUserRdd = sqlContext.sql(
            "select a.apkVersion,'historyAndcollect',count(a.userId),count(distinct a.userId),sum(a" +
              ".duration) from medusa_moretv_play_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where" +
              " a.userId not like '99999999999999%' and a.apkVersion='3.0.6'  and a.duration<=10800 and a" +
              ".duration>=0 and a.event ='playview' and a.launcherAccessLocationFromPath in ('history'," +
              "'collect') group by a.apkVersion").map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3),e.getLong
            (4))).collect()
          /*medusa 3.0.6 直播播放情况*/
          val medusaLivePlayUserRdd=sqlContext.sql("select a.apkVersion,count(a.userId),count(distinct a.userId) from " +
            "medusa_live_log as a join medusa_un_daily_update_log as b on a.userId=b.userId where a.apkVersion in ('3.0.6')  " +
            "and a.liveType='live' and a.event='startplay' group by a.apkVersion").map(e=>(e.getString(0),e.getLong(1),e
            .getLong(2))).map(e=>(e._1,(e._2,e._3)))
          val medusaLiveDurationRdd=sqlContext.sql("select a.apkVersion,sum(duration) from medusa_live_log as a join " +
            "medusa_un_daily_update_log as b on a.userId=b.userId where a.apkVersion in ('3.0.6') and a.liveType='live' " +
            "and a.duration >=0 and a.duration<=10800 group by a.apkVersion").map(e=>(e.getString(0),e.getLong(1))).map(e=>(e
            ._1,e._2))
          val medusaLiveUserRdd=medusaLivePlayUserRdd.join(medusaLiveDurationRdd).map(e=>(e._1,e._2._1._1,e._2._1._2,e._2
            ._2)).collect()




          /**
           * Moretv 2.6.7 播放统计
           *
           */
          sqlContext.read.parquet(s"/log/medusaAndMoretvMerger/$date/playview").registerTempTable("moretv_play_log")
          sqlContext.read.parquet(s"$moretvDir/$live/$date").registerTempTable("moretv_live_log")
          sqlContext.read.load(s"/log/medusa/mac2UserId/$date/moretvDailyUnUpdateActiveUserId")
            .registerTempTable("moretv_un_daily_update_log")
          /*moretv 2.6.7 整体播放数据*/
          val moretvPlayUserRdd = sqlContext.sql("select a.apkVersion,count(a.userId),count(distinct a.userId),sum(a" +
            ".duration) from moretv_play_log as a join moretv_un_daily_update_log as b on a.userId=b.userId " +
            "where a.userId not like '999999999999%' and a.apkVersion in ('2.6.7') and a.event='playview' and " +
            "a.duration<=10800 and a.duration>=0 group by a.apkVersion")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).collect()
          /*moretv 2.6.7 分类频道播放数据*/
          val moretvClassificationPlayUserRdd=sqlContext.sql("select a.apkVersion,count(a.userId),count(distinct a.userId)," +
            "sum(a.duration) from moretv_play_log as a join moretv_un_daily_update_log as b on a.userId=b.userId where a.apkVersion in ('2.6.7') " +
            "and a.userId not like '999999999999999999999%' and a.event ='playview' and a.duration >=0 and a.duration <=10800 " +
            "and a.launcherAreaFromPath='classification' and a.launcherAccessLocationFromPath in " +
            "('movie','tv','zongyi','sports','kids_home','xiqu','hot','jilu','mv','comic') group by a.apkVersion")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).collect()
          /*moretv 2.6.7 历史收藏播放数据*/
          val moretvHistoryAndCollectPlayUserRdd=sqlContext.sql("select a.apkVersion,count(a.userId),count(distinct a.userId)," +
            "sum(a.duration) from moretv_play_log as a join moretv_un_daily_update_log as b on a.userId=b.userId where a.apkVersion in ('2.6.7') and " +
            "a.userId not like '999999999999999999999%' and a.event ='playview' and a.duration >=0 and a.duration <=10800 " +
            "and a.launcherAccessLocationFromPath in ('history') group by a.apkVersion")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).collect()
          /*moretv 2.6.7 热门推荐播放数据*/
          val moretvRecommendPlayUserRdd=sqlContext.sql("select a.apkVersion,count(a.userId)," +
            "count(distinct a.userId),sum(a.duration) from moretv_play_log as a join moretv_un_daily_update_log as b on a.userId=b.userId " +
            "where a.apkVersion in ('2.6.7') and a.userId not like '999999999999999999999%' and a.event ='playview' and a" +
            ".duration >=0 and a.duration <=10800 and a.launcherAreaFromPath in ('hotrecommend') " +
            "group by a.apkVersion").map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))
            ).collect()
          /*moretv 2.6.7 直播播放数据*/
          val moretvLivePlayUserRdd=sqlContext.sql("select a.apkVersion,count(a.userId),count(distinct a.userId),sum(a" +
            ".duration) from moretv_live_log as a join moretv_un_daily_update_log as b on a.userId=b.userId where a.apkVersion in ('2.6.7')" +
            " and a.liveType='live' and a.duration >=0 and a.duration<=10800 group by a.apkVersion")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).collect()



          val insertSql = "insert into medusa_mtv_gray_testing_play_info(day,apk_version,type,play_num,play_user," +
            "total_duration) values (?,?,?,?,?,?)"


          medusaPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"All",new JLong(e._2),new JLong(e._3),new JLong(e._4))
          }
            )
          medusaRecommendlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"今日推荐",new JLong(e._3),new JLong(e._4),new JLong(e._5))
          })
          medusaClassificationPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"分类频道",new JLong(e._3),new JLong(e._4),new JLong(e._5))
          })
          medusaHistoryAndCollectPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"历史收藏",new JLong(e._3),new JLong(e._4),new JLong(e._5))
          })
          medusaLiveUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"直播",new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })

          moretvPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"All",new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })
          moretvRecommendPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"今日推荐",new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })
          moretvClassificationPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"分类频道",new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })
          moretvHistoryAndCollectPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"历史收藏",new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })
          moretvLivePlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,"直播",new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })


        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
