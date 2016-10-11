package com.moretv.bi.medusa.temp

import java.lang.{Double => JDouble, Integer => JInt, Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
/**
 * Created by HuZhehua on 2016/4/12.
 */
//MTV和Medusa平均用户使用时长分布和平均用户播放时长分布
//使用时长分布每20分钟一个区间，播放时长分布每十分钟一个区间
object UsageAndPlayDuration extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val pathMTV_exit = "/mbi/parquet/exit/"+date
          val pathMTV_play = "/mbi/parquet/playview/"+date
          val pathMDS_exit = "/log/medusa/parquet/"+date+"/exit"
          val pathMDS_play = "/log/medusa/parquet/"+date+"/play"
          import sqlContext.implicits._

          //mtv平均使用时长分布和平均观看时长分布
          val mtv_usage = sqlContext.read.load(pathMTV_exit).
            filter("duration > 0 and duration < 54000").select("duration","userId").map(row => {
            val duration = row.getInt(0)
            val userId = row.getString(1)
            val section = duration/1200+1
            (section.toLong,userId)
          }).toDF("section","userId")
          val mtv_usage_userNnum = mtv_usage.distinct().groupBy("section").count()
          val mtv_usageDur_distribution = mtv_usage.groupBy("section").count().join(mtv_usage_userNnum,"section").collect()

          val mtv_playDur = sqlContext.read.load(pathMTV_play).
            filter("duration > 0 and duration < 10800").select("duration","userId").map(row => {
            val duration = row.getInt(0)
            val userId = row.getString(1)
            val section = duration/600+1
            (section.toLong,userId)
          }).toDF("section","userId")
          val mtv_play_userNnum = mtv_playDur.distinct().groupBy("section").count()
          val mtv_playDur_distribution = mtv_playDur.groupBy("section").count().join(mtv_play_userNnum,"section").collect()

          //medusa平均使用时长分布和平均观看时长分布
          val mds_usageDur = sqlContext.read.load(pathMDS_exit).
            filter("duration > 0 and duration < 54000").select("duration","userId").map(row => {
            val duration = row.getLong(0)
            val userId = row.getString(1)
            val section = duration/1200+1
            (section,userId)
          }).toDF("section","userId")
          val mds_usage_userNnum = mds_usageDur.distinct().groupBy("section").count()
          val mds_usageDur_distribution = mds_usageDur.groupBy("section").count().join(mds_usage_userNnum,"section").collect()

          val mds_playDur = sqlContext.read.load(pathMDS_play).filter("event='userexit' or event='selfend'").
            filter("duration > 0 and duration < 10800").select("duration","userId").map(row => {
            val duration = row.getLong(0)
            val userId = row.getString(1)
            val section = duration/600+1
            (section,userId)
          }).toDF("section","userId")
          val mds_play_userNnum = mds_playDur.distinct().groupBy("section").count()
          val mds_playDur_distribution = mds_playDur.groupBy("section").count().join(mds_play_userNnum,"section").collect()

          if(p.deleteOld){
            val sqlDeleteUsage = "DELETE FROM usageDur_distribution WHERE day = ?"
            val sqlDeletePlay = "DELETE FROM playDur_distribution WHERE day = ?"
            util.delete(sqlDeleteUsage,day)
            util.delete(sqlDeletePlay,day)
          }

          val sqlInsertUsage = "INSERT INTO usageDur_distribution(day,tag,section,user_num,num) VALUES(?,?,?,?,?)"
          val sqlInsertPlay = "INSERT INTO playDur_distribution(day,tag,section,user_num,num) VALUES(?,?,?,?,?)"
          mtv_usageDur_distribution.foreach(row => {
            //println(row)
            util.insert(sqlInsertUsage, day, "mtv", new JLong(row.getLong(0)), new JLong(row.getLong(2)), new JLong(row.getLong(1)))
          })
          mtv_playDur_distribution.foreach(row =>
            util.insert(sqlInsertPlay,day,"mtv",new JLong(row.getLong(0)),new JLong(row.getLong(2)),new JLong(row.getLong(1)))
          )
          mds_usageDur_distribution.foreach(row =>
            util.insert(sqlInsertUsage,day,"medusa",new JLong(row.getLong(0)),new JLong(row.getLong(2)),new JLong(row.getLong(1)))
          )
          mds_playDur_distribution.foreach(row =>
            util.insert(sqlInsertPlay,day,"medusa",new JLong(row.getLong(0)),new JLong(row.getLong(2)),new JLong(row.getLong(1)))
          )
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
        util.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
    /*def sectionDistribution(df:DataFrame): Array = {
      import SQLContext.implicits._
      df.filter("duration > 0 and duration < 10800").select("duration").map(row => {
        val duration = row.getLong(0)
        val section = duration/1200+1
        section
      }).toDF("section").groupBy("section").count().collect()
    }*/
  }
}

