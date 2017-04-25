package com.moretv.bi.report.medusa.channeAndPrograma.mv

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by xiajun on 2016/5/16.
 * 推荐位节目、精选集播放情况
 */
object EachMVPlayTopBoard extends BaseClass{
  val regex = """(mv_category\*)(.+)""".r
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        sqlContext.udf.register("getSourceFromPath",getSourceFromPath _)
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.PLAY,date).select("userId","event","pathMain","videoSid","contentType","duration")
            .registerTempTable("log_data")

          val playRdd = sqlContext.sql("select videoSid,getSourceFromPath(pathMain),count(userId),count(distinct userId) " +
            "from log_data where event='startplay' and contentType='mv' group by videoSid,getSourceFromPath(pathMain)")
            .map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3))).filter(_._2!="").collect()
          val durationRdd = sqlContext.sql("select videoSid,getSourceFromPath(pathMain),sum(duration),count(distinct " +
            "userId) from log_data where event!='startplay' and contentType='mv' and duration between 0 and 10800 group by" +
            " videoSid,getSourceFromPath(pathMain)").map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3))).
            filter(_._2!="").collect()

          val insertSql = "insert into medusa_channel_mv_each_video_source_play_info(day,videoSid,title,source,play_num," +
            "play_user) values(?,?,?,?,?,?)"
          val insertSql1 = "insert into medusa_channel_mv_each_video_source_play_duration_info(day,videoSid,title,source," +
            "play_duration,play_user) values(?,?,?,?,?,?)"
          if(p.deleteOld){
            val deleteSql1="delete from medusa_channel_mv_each_video_source_play_info where day=?"
            val deleteSql2="delete from medusa_channel_mv_each_video_source_play_duration_info where day=?"
            util.delete(deleteSql1,insertDate)
            util.delete(deleteSql2,insertDate)
          }
          playRdd.foreach(e=>{
            try{
              util.insert(insertSql,insertDate,e._1,ProgramRedisUtil.getTitleBySid(e._1),e._2,new JLong(e._3),new JLong(e._4))
            }catch {
              case e:Exception =>
            }
          })
          durationRdd.foreach(e=>{
            try{
              util.insert(insertSql1,insertDate,e._1,ProgramRedisUtil.getTitleBySid(e._1),e._2,new JLong(e._3),new JLong(e._4))
            }catch {
              case e:Exception =>
            }
          })
        })
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

  def getSourceFromPath(path:String) = {
    if(path.contains("home*recommendation")){
      "launcher推荐"
    }else if(path.contains("mv*mvRecommendHomePage")){
      "音乐频道首页推荐"
    }else if(path.contains("search")){
      "搜索"
    }else if(path.contains("collect")){
      "收藏"
    }else if(path.contains("mv*mvCategoryHomePage")){
      var result = ""
      regex findFirstMatchIn path match {
        case Some(p) => result=p.group(2)
        case None =>
      }
      result
    }else ""
  }

}
