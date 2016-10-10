package com.moretv.bi.temp

import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
/**
 * Created by HuZhehua on 2016/4/12.
 */
//统计各频道每月用户平均播放总时长
object UserAvgPlayDurPerMonth extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "8g").set("spark.executor.cores", "5").set("spark.cores.max", "200")
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val path = "/mbi/parquet/playview/"+p.whichMonth+"*"
        val df = sqlContext.read.load(path).select("contentType","duration","userId").filter("duration > 0 and duration <= 10800").
          persist(StorageLevel.MEMORY_AND_DISK)
        val sumDur = df.map(row => {
          val contentType = row.getString(0)
          val duration = row.getInt(1).toLong
          (contentType, duration)
        }).reduceByKey((x,y) => x+y).collect().toMap
        val userNum = df.select("contentType","userId").distinct().groupBy("contentType").count().map(row => (row.getString(0),row.getLong(1))).collect().toMap
        userNum.foreach(x => {
          val contentType = x._1
          val userNum = x._2
          val duration = sumDur(contentType)
          println(contentType,userNum,duration,duration.toDouble/userNum)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}

