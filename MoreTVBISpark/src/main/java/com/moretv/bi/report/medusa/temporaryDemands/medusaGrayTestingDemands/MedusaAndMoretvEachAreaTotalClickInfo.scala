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
object MedusaAndMoretvEachAreaTotalClickInfo extends SparkSetting{
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

        val medusaAndMoretvDir="/log/medusaAndMoretvMerger"
        val logType = "homeaccess"

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaAndMoretvHomeaccessInput = s"$medusaAndMoretvDir/$date/$logType/"

          val medusaAndMoretvHomeaccesslog = sqlContext.read.parquet(medusaAndMoretvHomeaccessInput)

          medusaAndMoretvHomeaccesslog.select("userId","accessArea","apkVersion","locationIndex","event","buildDate","accessLocation")
            .registerTempTable("homeaccess_log")


          /*今日推荐点击*/
          val recommendRddSummary = sqlContext.sql("select '今日推荐',count(a.userId),count(distinct a.userId) " +
            "from homeaccess_log as a where a.userId not like '999999999999%' and  a.accessArea in ('recommendation','1')")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2))).collect()
          /*直播点击*/
          val liveClickRdd=sqlContext.sql("select '直播',count(a.userId),count(distinct a.userId) from homeaccess_log as a " +
            "where ((a.accessArea='5' and a.accessLocation='4') or a.accessArea='live') and a.userId not like " +
            "'9999999999%'").map(e=>(e.getString(0),e.getLong(1),e.getLong(2))).collect()
          /*分类频道点击*/
          val classificationRdd=sqlContext.sql("select '分类频道',count(a.userId),count(distinct a" +
            ".userId) from homeaccess_log as a where a.userId not like '99999999%' and (a.accessArea in ('my_tv'," +
            "'classification') and a.accessLocation not in ('history','collect')) or (a.accessArea='5' and " +
            "a.accessLocation in ('2','3','5','6','8','9','10','11','12','13','14'))").map(e=>(e.getString(0),e.getLong
            (1),e.getLong(2))).collect()
          /*历史收藏点击*/
          val historyAndCollectRdd=sqlContext.sql("select '历史收藏',count(a.userId),count(distinct a.userId) from  " +
            " homeaccess_log as a where a.userId not like '99999999%' and ((a.accessArea ='my_tv' and a.accessLocation " +
            "in ('history','collect')) or (a.accessArea='5' and a.accessLocation='1'))")
            .map(e=>(e.getString(0),e.getLong(1),e.getLong(2))).collect()


          val sqlInsert = "insert into medusa_mtv_gray_testing_click_info(day,apk_version," +
            "accessAreaName,click_num,click_user) values (?,?,?,?,?)"
          recommendRddSummary.foreach(e=>{
            util.insert(sqlInsert,insertDate,"All",e._1,new JLong(e._2),new JLong(e._3))
          })
          classificationRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,"All",e._1,new JLong(e._2),new JLong(e._3))
          })
          historyAndCollectRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,"All",e._1,new JLong(e._2),new JLong(e._3))
          })
          liveClickRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,"All",e._1,new JLong(e._2),new JLong(e._3))
          })




        })






      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
