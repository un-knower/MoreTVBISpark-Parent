package com.moretv.bi.report.medusa.tempAnalysis

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
object LiveDurationAnalytics1 extends SparkSetting{
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

          val medusaDailyActiveInput = s"$medusaDir/$date/live/"
          val moretvDailyActiveInput = s"$moretvDir/live/$date"

          val medusaDailyActivelog = sqlContext.read.parquet(medusaDailyActiveInput).select("userId","duration","liveType")
            .registerTempTable("log1")
          val moretvDailyActivelog = sqlContext.read.parquet(moretvDailyActiveInput).select("userId","duration","liveType")
            .registerTempTable("log2")

          val durationInfo=sqlContext.sql("select duration,count(userId) from log1 where liveType = 'live' and " +
            "duration between 10 and 86400 group by duration " +
            "union select duration,count(userId) from log2 where liveType='live' and duration between 10 and 86400 group " +
            "by duration").map(e=>(e.getLong(0),e.getLong(1))).reduceByKey(_+_)



          val sqlInsert = "insert into temp_live_duration_analytics(day,duration,num) values " +
            "(?,?,?)"

          durationInfo.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,new JLong(e._1),new JLong(e._2))
          })

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
