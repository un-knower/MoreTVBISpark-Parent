package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar
import java.lang.{Long=>JLong}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, DBOperationUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object MedusaAndMoretvPlayInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate


        val medusaDir = "/log/medusa/parquet"
        val moretvDir = "/mbi/parquet"
        val medusaAndMoretvDir = "/log/temp/medusaAndMoretvMerger/parquet"
        val enterLogType = "enter"
        val medusaPlayLogType = "playview"
        val moretvPlayLogType = "playview"

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaEnterInput = s"$medusaDir/$date/$enterLogType/"
          val medusaPlayInput = s"$medusaDir/$date/$medusaPlayLogType/"
          val moretvEnterInput = s"$moretvDir/$enterLogType/$date/"
          val moretvPlayInput = s"$moretvDir/$moretvPlayLogType/$date/"


          val medusaAndMoretvInput = s"$medusaAndMoretvDir/$date/$moretvPlayLogType"

          val medusaEnterlog = sqlContext.read.parquet(medusaEnterInput)
          val medusaPlaylog = sqlContext.read.parquet(medusaPlayInput)
          val moretvEnterlog = sqlContext.read.parquet(moretvEnterInput)
          val moretvPlaylog = sqlContext.read.parquet(moretvPlayInput)
          val medusaAndMoretvLog = sqlContext.read.parquet(medusaAndMoretvInput)

          medusaEnterlog.select("userId","apkVersion","buildDate").registerTempTable("medusa_enter_log")
          medusaPlaylog.select("userId","duration","apkVersion","event","contentType","buildDate")
            .registerTempTable("medusa_play_log")
          moretvEnterlog.select("userId","apkVersion").registerTempTable("moretv_enter_log")
          moretvPlaylog.select("userId","duration","apkVersion","event","contentType").registerTempTable("moretv_play_log")
          medusaAndMoretvLog.select("userId","duration","apkVersion","buildDate","launcherAreaFromPath","event",
            "launcherAccessLocationFromPath").registerTempTable("medusa_moretv_play_log")


          val medusaEnterUserRdd = sqlContext.sql("select count(userId),count(distinct userId) from " +
            "medusa_enter_log where userId not like '999999999999%' and apkVersion='3.0.6'")
            .map(e=>(e.getLong(0),e.getLong(1))).map(e=>(e._1,e._2)).collect()

          val medusaPlayUserRdd = sqlContext.sql(
            "select count(userId),count(distinct userId),sum(duration)" +
              " from medusa_play_log where userId not like '99999999999999%' and apkVersion='3.0.6' and" +
              " duration<=10800 and duration>=0 and event='playview'")
            .map(e=>(e.getLong(0),e.getLong(1),e.getLong(2))).map(e=>(e._1,e._2,e._3)).collect()


          val medusaEachAreaPlayUserRdd = sqlContext.sql(
            "select launcherAreaFromPath,count(a.userId),count(distinct a.userId),sum(a.duration)" +
              " from medusa_moretv_play_log as a where a.userId not " +
              "like '99999999999999%' and a.apkVersion='3.0.6' and a.duration<=10800 and a" +
              ".duration>=0 and a.event ='playview' and launcherAreaFromPath in ('classification'," +
              "'recommendation','my_tv','foundation') group by launcherAreaFromPath")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e
            .getLong(3))).map(e=>(e._1,e._2,e._3,e._4)).collect()

          val medusaClassificationPlayUserRdd = sqlContext.sql(
            "select 'classificationAndMytv',count(a.userId),count(distinct a.userId),sum(a" +
              ".duration)" +
              " from medusa_moretv_play_log as a where a.userId not " +
              "like '99999999999999%' and a.apkVersion='3.0.6' and a.duration<=10800 and a" +
              ".duration>=0 and a.event ='playview' and launcherAreaFromPath in ('classification','my_tv') " +
              "and launcherAccessLocationFromPath not in ('account','history','collect') ")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e
            .getLong(3))).map(e=>(e._1,e._2,e._3,e._4)).collect()


          val medusaHistoryAndCollectPlayUserRdd = sqlContext.sql(
            "select 'historyAndcollect',count(a.userId),count(distinct a.userId),sum(a" +
              ".duration)" +
              " from medusa_moretv_play_log as a where a.userId not " +
              "like '99999999999999%' and a.apkVersion='3.0.6'  and a.duration<=10800 and a" +
              ".duration>=0 and a.event ='playview' and launcherAccessLocationFromPath in ('history'," +
              "'collect')")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e
            .getLong(3))).map(e=>(e._1,e._2,e._3,e._4)).collect()

          val medusaHistoryCollectPlayUserRdd = sqlContext.sql(
            "select launcherAccessLocationFromPath,count(a.userId),count(distinct a.userId),sum(a" +
              ".duration)" +
              " from medusa_moretv_play_log as a where a.userId not " +
              "like '99999999999999%' and a.apkVersion='3.0.6'  and a.duration<=10800 and a" +
              ".duration>=0 and a.event ='playview' and launcherAccessLocationFromPath in ('history'," +
              "'collect') group by launcherAccessLocationFromPath")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e
            .getLong(3))).map(e=>(e._1,e._2,e._3,e._4)).collect()



          val medusaEachContentTypePlayUserRdd = sqlContext.sql(
            "select contentType,count(userId),count(distinct userId),sum(duration)" +
              " from medusa_play_log where userId not like " +
              "'99999999999999%' and apkVersion='3.0.6' and duration<=10800 and duration>=0" +
              " and event='playview' group by contentType")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).map(e=>(e._1,e._2,e._3,e._4)).collect()

          val moretvEnterUserRdd = sqlContext.sql("select count(userId),count(distinct userId) from " +
            "moretv_enter_log where userId not like '999999999999%' and apkVersion='2.6.7'")
            .map(e=>(e.getLong(0),e.getLong(1))).map(e=>(e._1,e._2)).collect()
          val moretvPlayUserRdd = sqlContext.sql("select count(userId),count(distinct userId),sum(duration) from " +
            "moretv_play_log where userId not like '999999999999%' and apkVersion='2.6.7' and event='playview' and " +
            "duration<=10800 and duration>=0")
            .map(e=>(e.getLong(0),e.getLong(1),e.getLong(2))).map(e=>(e._1,e._2,e._3)).collect()

          val moretvEachContentTypePlayUserRdd = sqlContext.sql("select contentType,count(userId),count(distinct userId)," +
            "sum(duration) from moretv_play_log where userId not like '999999999999%' and apkVersion='2.6.7' and " +
            "duration<=10800 and duration>=0 and event='playview' group by contentType")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).map(e=>(e._1,e._2,e._3,e._4)).collect()



          val sqlInsert = "insert into medusa_gray_testing_play_info_each_day(date,apk_version,play_num,play_user," +
            "play_duration,enter_num,enter_user) values (?,?,?,?,?,?,?)"
          val sqlInsert1 = "insert into medusa_gray_testing_each_contentType_play_info_each_day(date,apk_version," +
            "contentType,play_num, play_user,play_duration) values (?,?,?,?,?,?)"
          val sqlInsert2 = "insert into medusa_gray_testing_each_area_play_info_each_day(date,apk_version,area,play_num, " +
            "play_user,play_duration) values (?,?,?,?,?,?)"


          util.insert(sqlInsert,insertDate,"3.0.6",new JLong(medusaPlayUserRdd(0)._1),new JLong(medusaPlayUserRdd
            (0)._2), new JLong(medusaPlayUserRdd(0)._3),new JLong(medusaEnterUserRdd(0)._1),new JLong(medusaEnterUserRdd(0)._2))

          util.insert(sqlInsert,insertDate,"2.6.7", new JLong(moretvPlayUserRdd(0)._1),
            new JLong(moretvPlayUserRdd(0)._2),new JLong(moretvPlayUserRdd(0)._3),new JLong
            (moretvEnterUserRdd(0)._1),new JLong(moretvEnterUserRdd(0)._2))

          moretvEachContentTypePlayUserRdd.foreach(r=>{
            util.insert(sqlInsert1,insertDate,"2.6.7",r._1,new JLong(r._2),new JLong(r._3),new JLong(r._4))
          })
          medusaEachContentTypePlayUserRdd.foreach(r=>{
            util.insert(sqlInsert1,insertDate,"3.0.6",r._1,new JLong(r._2),new JLong(r._3),new JLong(r._4))
          })


          medusaEachAreaPlayUserRdd.foreach(r=>{
              util.insert(sqlInsert2,insertDate,"3.0.6",r._1,new JLong(r._2),new JLong(r._3),new JLong(r._4))
            })
          medusaHistoryAndCollectPlayUserRdd.foreach(r=>{
            util.insert(sqlInsert2,insertDate,"3.0.6",r._1,new JLong(r._2),new JLong(r._3),new JLong(r._4))
          })
          medusaClassificationPlayUserRdd.foreach(r=>{
            util.insert(sqlInsert2,insertDate,"3.0.6",r._1,new JLong(r._2),new JLong(r._3),new JLong(r._4))
          })
          medusaHistoryCollectPlayUserRdd.foreach(r=>{
            util.insert(sqlInsert2,insertDate,"3.0.6",r._1,new JLong(r._2),new JLong(r._3),new JLong(r._4))
          })





        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
