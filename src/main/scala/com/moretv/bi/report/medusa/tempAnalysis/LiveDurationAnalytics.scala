package com.moretv.bi.report.medusa.tempAnalysis

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object LiveDurationAnalytics extends SparkSetting{
  def main(args: Array[String]) {
    if(args.length>0) {
      val sc = new SparkContext(config)
      implicit val sqlContext = new SQLContext(sc)
      val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
      val time = args(0)
      val dir = s"/log/medusaAndMoretvMerger/$time/live"
      sqlContext.read.parquet(dir).select("duration","userId","event","liveType").registerTempTable("log")
      val df = sqlContext.sql("select duration,count(userId),count(distinct userId) from log where event = 'switchchannel'" +
        " and liveType='live' group by duration")
      val insertSql = "insert into temp_live_duration_analytics1(duration,num,user) values(?,?,?)"
      df.map(e=>(e.getLong(0),e.getLong(1),e.getLong(2))).collect().foreach(e=>{
        util.insert(insertSql,new JLong(e._1),new JLong(e._2), new JLong(e._3))
      })
    }
  }
}
