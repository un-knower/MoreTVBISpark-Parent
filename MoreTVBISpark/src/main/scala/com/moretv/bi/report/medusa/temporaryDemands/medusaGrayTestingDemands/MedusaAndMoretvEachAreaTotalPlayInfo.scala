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
object MedusaAndMoretvEachAreaTotalPlayInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate


        val medusaAndMoretvDir="/log/medusaAndMoretvMerger"
        val logType = "playview2"
        val live="live"

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaAndMoretvPlayInput = s"$medusaAndMoretvDir/$date/$logType/"


          val medusaAndMoretvPlayLog = sqlContext.read.parquet(medusaAndMoretvPlayInput)
            .registerTempTable("total_play_log")

          sqlContext.read.parquet(s"$medusaAndMoretvDir/$date/$live").registerTempTable("total_live_log")



          /*今日推荐播放情况*/
          val recommendlayUserRdd = sqlContext.sql(
            "select '今日推荐',count(a.userId),count(distinct a.userId),sum(a.duration)" +
              " from total_play_log as a where a.userId not like '99999999999999%' and a.duration<=10800 and a" +
              ".duration>=0 and a.event ='playview' and a.launcherAreaFromPath in ('recommendation','hotrecommend')")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).collect()

          /*分类播放情况*/
          val classificationPlayUserRdd = sqlContext.sql(
            "select '分类频道',count(a.userId),count(distinct a.userId),sum(a" +
              ".duration) from total_play_log as a where a.userId not like '99999999999999%' and " +
              "a.duration<=10800 and a.duration>=0 and a.event ='playview' and " +
              "((a.launcherAreaFromPath in ('classification','my_tv') and a.launcherAccessLocationFromPath not in " +
              "('account','history','collect')) or (a.launcherAreaFromPath='classification' and " +
              "a.launcherAccessLocationFromPath in " +
              "('movie','tv','zongyi','sports','kids_home','xiqu','hot','jilu','mv','comic')))")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).collect()

          /*历史收藏播放情况*/
          val historyAndCollectPlayUserRdd = sqlContext.sql(
            "select '历史收藏',count(a.userId),count(distinct a.userId),sum(a" +
              ".duration) from total_play_log as a where a.userId not like '99999999999999%' and" +
              " a.duration<=10800 and a.duration>=0 and a.event ='playview' and a.launcherAccessLocationFromPath in ('history'," +
              "'collect')").map(e=>(e.getString(0),e.getLong(1),e.getLong(2),e.getLong(3))).collect()
          /*直播播放情况*/
          val livePlayUserRdd=sqlContext.sql("select '直播' as channel,count(a.userId),count(distinct a.userId) from " +
            "total_live_log as a where a.liveType='live' and a.event!='switchchannel'").map(e=>(e.getString(0),e.getLong(1),e
            .getLong(2))).map(e=>(e._1,(e._2,e._3)))
          val liveDurationRdd=sqlContext.sql("select '直播' as channel,sum(duration) from total_live_log as a where" +
            " a.liveType='live' and a.duration >=0 and a.duration<=10800 and a.event!='startplay'").map(e=>(e.getString(0),e
            .getLong(1))).map(e=>(e._1,e._2))
          val liveUserRdd=livePlayUserRdd.join(liveDurationRdd).map(e=>(e._1,e._2._1._1,e._2._1._2,e._2
            ._2)).collect()



          val insertSql = "insert into medusa_mtv_gray_testing_play_info(day,apk_version,type,play_num,play_user," +
            "total_duration) values (?,?,?,?,?,?)"


          recommendlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,"All",e._1,new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })
          classificationPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,"All",e._1,new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })
          historyAndCollectPlayUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,"All",e._1,new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })
          liveUserRdd.foreach(e=>{
            util.insert(insertSql,insertDate,"All",e._1,new JLong(e._2),new JLong(e._3),new JLong(e._4))
          })


        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
