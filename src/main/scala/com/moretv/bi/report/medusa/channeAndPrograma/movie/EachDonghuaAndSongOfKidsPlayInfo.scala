package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by xiajun on 2016/5/16.
 * 统计少儿频道每个动画片与儿歌的播放情况！
 */
object EachDonghuaAndSongOfKidsPlayInfo extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.PLAYVIEW,date).select("userId","path","pathMain","event","contentType",
            "videoSid").registerTempTable("log_data")

          val donghuaRdd = sqlContext.sql("select videoSid,count(userId),count(distinct userId)" +
            " from log_data where event in ('startplay','playview') and (path not like '%kids_songhome%' or " +
            "pathMain not like '%kids_rhymes%') and contentType='kids' group by videoSid").
            map(e=>(e.getString(0),e.getLong(1),e.getLong(2)))

          val songRdd = sqlContext.sql("select videoSid,count(userId),count(distinct userId) from log_data where event in " +
            "('startplay','playview') and (path like '%kids_songhome%' or pathMain like '%kids_rhymes%') " +
            "and contentType='kids' group by videoSid").
            map(e=>(e.getString(0),e.getLong(1),e.getLong(2)))


          val insertSql="insert into medusa_channel_kids_eachdonghua_and_song_play_info(day,type,video_sid,title," +
            "play_num,play_user) values (?,?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql="delete from medusa_channel_kids_eachdonghua_and_song_play_info where day=?"
            util.delete(deleteSql,insertDate)
          }


          donghuaRdd.collect().foreach(e=>{
            try{

              util.insert(insertSql,insertDate,"donghua",e._1,ProgramRedisUtil.getTitleBySid(e._1),new JLong(e._2),
                new JLong(e._3))
            }catch {
              case e:Exception => throw e
            }
          })

          songRdd.collect().foreach(e=>{
            try {
              util.insert(insertSql,insertDate,"song",e._1,ProgramRedisUtil.getTitleBySid(e._1),new JLong(e._2),new JLong(e
                ._3))
            }catch {
              case e:Exception => throw  e
            }

          })

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
