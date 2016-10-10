package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object SportPlayAndLiveTotalUserInfo extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(SportPlayAndLiveTotalUserInfo,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate


        val medusaDir = "/log/medusa/parquet"

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          val playDir=s"$medusaDir/$date/play"
          val liveDir=s"$medusaDir/$date/live"

          sqlContext.read.parquet(playDir).registerTempTable("log")
          sqlContext.read.parquet(liveDir).registerTempTable("log1")
          val sportPlayUserRdd=sqlContext.sql("select distinct userId from log where contentType = 'sports'" +
            " and event ='startplay'").map(e=>e.getString(0))
          val sportLiveUserRdd=sqlContext.sql("select distinct userId from log1 where channelSid like '%sport%'").map(e=>e
            .getString(0))
          val totalUserRdd=sportLiveUserRdd.union(sportPlayUserRdd)
          val totalUserNum=totalUserRdd.distinct().count()



          val sqlInsert = "insert into medusa_channel_play_and_live_user_sport_info(day,total_user) values (?,?)"

          util.insert(sqlInsert,insertDate,new JLong(totalUserNum))





        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
