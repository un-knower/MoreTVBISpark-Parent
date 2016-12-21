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
object SportsOlympicLiveCenterEachMatchLiveInfo extends BaseClass{

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

          sqlContext.read.parquet(playviewInput).select("userId","pathMain","event","liveType","duration","channelSid",
            "liveName")
            .registerTempTable("log_data")

          val playInfoArr = sqlContext.sql("select channelSid,liveName,count(userId),count(distinct userId) from " +
            "log_data where event ='startplay' and liveType='live' and pathMain like '%olympic_livecenter%' and length" +
            "(liveName)<100 group by channelSid,liveName").
            map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3))).collect()
          val playDurationArr = sqlContext.sql("select channelSid,liveName,sum(duration),count(distinct " +
            "userId) from log_data where event='switchchannel' and liveType = 'live' and pathMain like " +
            "'%olympic_livecenter%' and length(liveName)<100 " +
            "and duration>=0 and duration<=21600 group by channelSid,liveName").
            map(e=>(e.getString(0),e.getString(1),e.getLong(2)
            ,e.getLong(3))).collect()

          val insertSql = "insert into medusa_channel_sport_olympic_live_center_each_match_live_info(day,channelSid," +
            "liveName,live_num,live_user,total_duration,total_dur_user) values (?,?,?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql = "delete from medusa_channel_sport_olympic_live_center_each_match_live_info where day=?"
            util.delete(deleteSql,insertDate)
          }
          (0 until playInfoArr.length).foreach(i=>{
            try{
              util.insert(insertSql,insertDate,playInfoArr(i)._1,playInfoArr(i)._2,new JLong(playInfoArr(i)._3),new JLong
              (playInfoArr(i)._4), new JLong(playDurationArr(i)._3),new JLong(playDurationArr(i)._4))
            }catch {
              case e:Exception =>
            }

          })
        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }


}
