package com.moretv.bi.report.medusa.channeAndPrograma.mv.bf310

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by xiajun on 2016/5/16.
  * 推荐位节目、精选集播放情况
  */
object MVRecommendPlay extends BaseClass {
  def main(args: Array[String]): Unit = {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(MVRecommendPlay, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val medusaDir = "/log/medusa/parquet"
        val calendar = Calendar.getInstance()
        val s = sqlContext
        import s.implicits._
        sqlContext.udf.register("getSidFromPath", getSidFromPath _)
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          val playviewInput = s"$medusaDir/$date/play/"

          sqlContext.read.parquet(playviewInput).select("userId", "event", "pathMain", "duration", "videoSid")
            .registerTempTable("log_data")

          sqlContext.sql("select userId,case when duration is null then 0 else duration end,event,getSidFromPath(pathMain),videoSid from log_data where pathMain like" +
            " '%mv*mvRecommendHomePage*%' and pathMain not like '%mv_station%'").map(e => (e.getString(0), e.getLong(1), e
            .getString(2), e.getString(3), e.getString(4))).filter(_._4 != null).toDF("userId", "duration", "event", "subjectCode",
            "videoSid").registerTempTable("log")

          val subjectPlayInfo = sqlContext.sql("select subjectCode,count(userId),count(distinct " +
            "userId) from log where subjectCode!=videoSid and event='startplay' group by subjectCode").
            map(e => (e.getString(0), e.getLong(1), e.getLong(2))).collect()

          val programPlayInfo = sqlContext.sql("select videoSid,count(userId),count(distinct userId) from log where " +
            "subjectCode=videoSid and event='startplay' group by videoSid").map(e => (e.getString(0),
            e.getLong(1), e.getLong(2))).collect()

          val subjectDurationInfo = sqlContext.sql("select subjectCode,sum(duration),count(distinct " +
            "userId) from log where subjectCode!=videoSid and event!='startplay' and duration between 0 and 10800 group by" +
            " subjectCode").map(e => (e.getString(0), e.getLong(1), e.getLong(2))).collect()

          val programDurationInfo = sqlContext.sql("select videoSid,sum(duration),count(distinct userId) from log where " +
            "subjectCode=videoSid and event!='startplay' and duration between 0 and 10800 group by videoSid").map(e => (e
            .getString(0), e.getLong(1), e.getLong(2))).collect()


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
            util.insert(insertSql1, insertDate, "topic", i._1, LiveCodeToNameUtils.getMVSubjectName(i._1), new JLong(i._2), new JLong(i._3))
          })
          programPlayInfo.foreach(i => {
            util.insert(insertSql1, insertDate, "program", i._1, ProgramRedisUtil.getTitleBySid(i._1), new JLong(i._2), new JLong(i._3))
          })
          subjectDurationInfo.foreach(i => {
            util.insert(insertSql2, insertDate, "topic", i._1, LiveCodeToNameUtils.getMVSubjectName(i._1), new JLong(i._2), new JLong(i._3))
          })
          programDurationInfo.foreach(i => {
            util.insert(insertSql2, insertDate, "program", i._1, ProgramRedisUtil.getTitleBySid(i._1), new JLong(i._2), new JLong(i._3))
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
