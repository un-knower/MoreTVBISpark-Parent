package com.moretv.bi.report.medusa.channeAndPrograma.sports

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
  * Created by albert-fang on 16-8-2.
  * 统计比赛的播放人数次数以及人均播放时长
  */
object MatchPlayStatisticsDWVersion extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(MatchPlayStatistics,args)
  }

  override def execute(args: Array[String]): Unit = {

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
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val playDir=s"$medusaDir/$date/play"
          sqlContext.read.parquet(playDir).registerTempTable("log_data")
          val rdd=sqlContext.sql("select aa.videoSid,aa.pathMain,aa.user_num,aa.access_num,bb.watch_time/aa.user_num as `per_play` " +
            "from (select videoSid, pathMain, count(distinct userId) as `user_num`, count(userId) as `access_num` from " +
            "(select videoSid,userId,case when pathMain like '%sports*horizontal%' then '体育首页' when pathMain like '%sports*League%' then '联赛' " +
            "when pathMain like '%sports*horizontal*collect%' then '收藏' end as `pathMain` from log_data where contentType = 'sports' and event = 'startplay')a " +
            "where pathMain != 'null' group by videoSid, pathMain)aa join (select videoSid, pathMain, sum(duration) as `watch_time` from " +
            "(select videoSid,userId,case when pathMain like '%sports*horizontal%' then '体育首页' when pathMain like '%sports*League%' then '联赛' " +
            "when pathMain like '%sports*horizontal*collect%' then '收藏' end as `pathMain`,duration from log_data where contentType = 'sports' " +
            "and event != 'startplay' and duration >= 0 and duration <= 10800)b where pathMain != 'null' group by videoSid, pathMain)bb " +
            "on aa.videoSid = bb.videoSid and aa.pathMain = bb.pathMain").map(e=>(e.getString(0),e.getString(1),e.getLong
          (2),e.getLong(3),e.getDouble(4))).persist(StorageLevel.MEMORY_AND_DISK)

          /*数据库操作*/
          if(p.deleteOld){
            val deleteSql = "delete from medusa_channel_sport_match_play_info where day = ?"
            util.delete(deleteSql,insertDate)
          }
          val sqlInsert = "insert into medusa_channel_sport_match_play_info(day,video_sid,video_title,path_main,user_num,access_num,per_play) " +
            "values (?,?,?,?,?,?,?)"

          rdd.collect().foreach(
            e=>{
              util.insert(sqlInsert,insertDate,e._1,ProgramRedisUtil.getTitleBySid(e._1),e._2,new JLong(e._3),new JLong(e._4),new JDouble(e._5))
            }
          )

          rdd.unpersist()
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }

  }
}
