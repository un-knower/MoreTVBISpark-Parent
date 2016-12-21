package com.moretv.bi.report.medusa.channeAndPrograma.sports

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/7/27.
 * 统计奥运直播中心播放人数、次数与时长
 *
 */
object SportsOlympicLiveCenterLiveInfo extends BaseClass{

  def main(args: Array[String]): Unit = {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(this,args)
  }

  def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaDir = "/log/medusa/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          val playviewInput = s"$medusaDir/$date/live/"

          sqlContext.read.parquet(playviewInput).select("userId","pathMain","event","duration","liveType")
            .registerTempTable("log_data")

          val playInfoRdd = sqlContext.sql("select count(userId),count(distinct userId) from " +
            "log_data where event ='startplay' and liveType='live' and pathMain like '%olympic_livecenter%'").
            map(e=>("liveCenter",(e.getLong(0),e.getLong(1))))
          val playDurationRdd = sqlContext.sql("select sum(duration),count(distinct " +
            "userId) from log_data where event = 'switchchannel' and liveType='live' and pathMain like " +
            "'%olympic_livecenter%' and duration>=0 and " +
            "duration<=21600").map(e=>("liveCenter",(e.getLong(0),e.getLong(1))))

          val insertSql = "insert into medusa_channel_sport_olympic_live_center_live_info(day,live_num,live_user," +
            "total_duration,total_dur_user) values (?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql = "delete from medusa_channel_sport_olympic_live_center_live_info where day=?"
            util.delete(deleteSql,insertDate)
          }
          (playInfoRdd join playDurationRdd).collect.foreach(e=>{
             util.insert(insertSql,insertDate,new JLong(e._2._1._1),new JLong(e._2._1._2),new JLong(e._2._2._1),
               new JLong(e._2._2._2))
           })


        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

}
