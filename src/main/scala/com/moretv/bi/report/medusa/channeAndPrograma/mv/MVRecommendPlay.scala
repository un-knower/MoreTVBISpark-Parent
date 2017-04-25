package com.moretv.bi.report.medusa.channeAndPrograma.mv

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.bi.utils.ElasticSearchUtil
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by xiajun on 2016/5/16.
  * 推荐位节目、精选集播放情况
  */
object MVRecommendPlay extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        val s = sqlContext
        import s.implicits._
        sqlContext.udf.register("getSidFromPath", getSidFromPath _)
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.PLAY,date).select("userId", "event", "pathMain", "duration", "videoSid")
            .registerTempTable("log_data")

          sqlContext.sql("select userId,case when duration is null then 0 else duration end,event,getSidFromPath(pathMain),videoSid from log_data where pathMain like" +
            " '%mv*mvRecommendHomePage*%' and pathMain not like '%mv_station%'")
            .map(e => (e.getString(0), e.getLong(1), e.getString(2), e.getString(3), e.getString(4)))
            .filter(_._4 != null)
            .toDF("userId", "duration", "event", "subjectCode",
            "videoSid").registerTempTable("log")

          DataIO.getDataFrameOps.getDimensionDF(
            sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_MV_TOPIC
          ).registerTempTable("dim_mv_topic")

          DataIO.getDataFrameOps.getDimensionDF(
            sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM
          ).registerTempTable("dim_program")

          val subjectPlayInfo = sqlContext.sql(
            "select a.subjectCode, a.pv, a.uv, b.mv_topic_name from " +
              "(select subjectCode,count(userId) pv,count(distinct userId) uv " +
              "from log where subjectCode != videoSid and event= 'startplay' group by subjectCode) a " +
              "left join dim_mv_topic b on a.subjectCode = b.mv_topic_sid and b.dim_invalid_time is null"
          ).map(e => (e.getString(0), e.getLong(1), e.getLong(2), e.getString(3))).collect()

          val programPlayInfo = sqlContext.sql(
            "select a.videoSid, a.pv, a.uv, b.title from " +
              "(select videoSid,count(userId) pv, count(distinct userId) uv from log where " +
              "subjectCode=videoSid and event='startplay' group by videoSid ) a " +
              "left join dim_program b on a.videoSid = b.sid and b.dim_invalid_time is null"
          ).map(e => (e.getString(0), e.getLong(1), e.getLong(2), e.getString(3))).collect()

          val subjectDurationInfo = sqlContext.sql(
            "select a.subjectCode, a.duration, a.uv, b.mv_topic_name from " +
              "(select subjectCode, sum(duration) duration, count(distinct userId) uv from log " +
              "where subjectCode!=videoSid and event!='startplay' and duration between 0 and 10800 group by subjectCode) a " +
              "left join dim_mv_topic b on a.subjectCode = b.mv_topic_sid and b.dim_invalid_time is null"
          ).map(e => (e.getString(0), e.getLong(1), e.getLong(2), e.getString(3))).collect()

          val programDurationInfo = sqlContext.sql(
            "select a.videoSid, a.duration, a.uv, b.title from " +
              "(select videoSid, sum(duration) duration, count(distinct userId) uv from log where " +
              "subjectCode=videoSid and event!='startplay' and duration between 0 and 10800 group by videoSid) a " +
              "left join dim_program b on a.videoSid = b.sid and b.dim_invalid_time is null"
          ).map(e => (e
            .getString(0), e.getLong(1), e.getLong(2), e.getString(3))).collect()


          val insertSql1 = "insert into medusa_channel_mv_recommend_each_program_play_info(day,flag,programCode,title," +
            "play_num,play_user) values (?,?,?,?,?,?)"
          val insertSql2 = "insert into medusa_channel_mv_recommend_each_program_play_duration_info(day,flag,programCode," +
            "title,play_duration,play_user) values (?,?,?,?,?,?)"

          if (p.deleteOld) {
            val deleteSql1 = "delete from medusa_channel_mv_recommend_each_program_play_info where day=?"
            val deleteSql2 = "delete from medusa_channel_mv_recommend_each_program_play_duration_info where day=?"
            util.delete(deleteSql1, insertDate)
            util.delete(deleteSql2, insertDate)
          }

          subjectPlayInfo.foreach(i => {
            try{
              util.insert(insertSql1, insertDate, "topic", i._1, i._4, new JLong(i._2), new JLong(i._3))
            }catch {
              case e:Exception =>{}
            }
          })
          programPlayInfo.foreach(i => {
            try{
              util.insert(insertSql1, insertDate, "program", i._1, i._4, new JLong(i._2), new JLong(i._3))
            }catch {
              case e:Exception => {}
            }
          })
          subjectDurationInfo.foreach(i => {
            try{
              util.insert(insertSql2, insertDate, "topic", i._1, i._4, new JLong(i._2), new JLong(i._3))
            }catch {
              case e:Exception => {}
            }
          })
          programDurationInfo.foreach(i => {
            try{
              util.insert(insertSql2, insertDate, "program", i._1, ProgramRedisUtil.getTitleBySid(i._1), new JLong(i._2), new JLong(i._3))
            }catch {
              case e:Exception => {}
            }
          })
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

  def getSidFromPath(path: String) = {
    val splitPath = path.split("\\*")
    val lastInfo = splitPath(splitPath.length - 1)
    if (lastInfo.length == 12) {
      lastInfo
    } else null
  }

}
