package com.moretv.bi.temp.live

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by laishun on 15/10/9.
 */
object HomeLive extends SparkSetting with DateUtil{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        println(p)
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)

        val path = "/mbi/parquet/live/"+p.startDate
        val df = sqlContext.read.load(path)
        df.registerTempTable("log_data")
        val date = DateFormatUtils.toDateCN(p.startDate, -1)
        val start = p.startTime
        val end = p.endTime
        val logRdd = sqlContext.sql("select userId,duration from log_data " +
          s"where channelSid = '${p.sid}' and path like 'home-hotrecommend-%' " +
          s"and datetime between '$date $start' and '$date $end'").cache()
//          "where channelSid = 's9n8op9wxyab' and path like 'home-hotrecommend-%'").cache()

        val times = logRdd.selectExpr("count(userId)").first().getLong(0)
        val user = logRdd.selectExpr("count(distinct userId)").first().getLong(0)
        logRdd.registerTempTable("duration_data")
        val avgDuration = sqlContext.sql("select sum(duration)/count(distinct userId) " +
          "from duration_data where duration > 0 and duration < 10800").first().getDouble(0)

        println(s"times:\t$times")
        println(s"user:\t$user")
        println(s"avgDuration:\t$avgDuration")
        logRdd.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

}
