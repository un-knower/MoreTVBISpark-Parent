package com.moretv.bi.template.StatHandler

import java.util.Calendar

import com.moretv.bi.temp.whatsup._
import com.moretv.bi.template.StatHandler
import com.moretv.bi.util.DateFormatUtils
import com.moretv.bi.util.FileUtils._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.lag

/**
  * Created by witnes on 11/3/16.
  */
object PlayLiveExecutor {


  def loadSourceData(logType: String, loadDate: String) = {

    val loadPath = s"/log/medusaAndMoretvMerger/$loadDate/live"

    //SqlPattern.timeType = SqlPattern.TIMETYPE.get(logType).get

    sqlContext.read.parquet(loadPath)
      .filter("duration between 0 and 10800")
      .selectExpr("datetime", "substr(apkVersion,1,1) as version", "userId", "event", "channelSid", "duration", "pathMain")
      .registerTempTable("log_data")

  }

  /**
    *
    * @param schemaString
    * @param loadPath
    */
  def loadFilterCondition(schemaString: String, loadPath: String) = {

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row

    val cal = Calendar.getInstance

    val rdd = sc.textFile(loadPath)
      .map(_.split("\t"))
      .map {
        attr =>
          cal.setTime(DateFormatUtils.readFormat.parse(attr(0)))
          Row(attr(1), attr(2),
            DateFormatUtils.cnFormat.format(cal.getTime) + " " + attr(3),
            DateFormatUtils.cnFormat.format(cal.getTime) + " " + attr(4))
      }

    val fields = schemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    sqlContext.createDataFrame(rdd, schema).registerTempTable("log_ref")

    sqlContext.sql("select * from log_ref").show(100, false)

  }

  def withoutVersionStat(outputFile: String) = {

    val df = sqlContext.sql(
      """
        |SELECT tbl_2.startTime, tbl_2.endTime,  tbl_2.channelSid, tbl_2.channelName,
        | count(tbl_1.userId) as pv, count(distinct tbl_1.userId) as uv, tbl_1.version
        |FROM
        | (
        |   SELECT datetime, channelSid, userId, version
        |   FROM log_data
        |   WHERE event in ('startplay')
        | ) tbl_1
        | JOIN
        | (
        |   SELECT *
        |   FROM log_ref
        | ) tbl_2
        | ON tbl_1.channelSid = tbl_2.channelSid and tbl_1.datetime between tbl_2.startTime and tbl_2.endTime
        |GROUP BY tbl_2.channelSid, tbl_2.channelName, tbl_2.startTime, tbl_2.endTime, tbl_1.version
      """.stripMargin)

    withCsvWriterOld(outputFile) {
      out => {
        df.collect.foreach(e => {
          out.println(e.getString(0), e.getString(1), e.getString(2), e.getString(3), e.getLong(4), e.getLong(5))
        })
      }
    }
  }

  //  def withVersionStat() = {
  //
  //    sqlContext.sql(
  //      """
  //        |SELECT
  //        |FROM (
  //        |(
  //        | SELECT videoSid, videoName, startTime, endTime
  //        | FROM log_ref
  //        |) tbl_0
  //        |JOIN
  //        |(
  //        | SELECT datetime, version, videoSid, userId
  //        | FROM log_data
  //        | WHERE event in ('playview', 'startplay')
  //        |) tbl_1
  //        |ON tbl_0.videoSid = tbl_1.videoSid AND tbl_1.datetime between tbl_0.StartTime and tbl_0.endTime
  //        |JOIN
  //        |SELECT *
  //        |FROM (
  //        |
  //        |SELECT tbl_2.version, tbl_1.videoSid, tbl_2.startTime, tbl_2.endTime ,
  //        |sum(tbl_2,duration) / count(distinct userId) as mean_dur
  //        |FROM
  //        | (
  //        |   SELECT videoSid, videoName, startTime, endTime
  //        |   FROM log_ref
  //        | ) tbl_0
  //        | JOIN
  //        | (
  //        |   SELECT datetime, version, videoSid, duration, userId
  //        |   FROM log_data
  //        |   WHERE event not in ('playview', 'startplay')
  //        | ) tbl_2
  //        | ON tbl_0.videoSid = tbl_2.videoSid AND tbl_2.datetime between tbl_0.StartTime and tbl_0.endTime
  //        |)
  //        |GROUP BY tbl1.videoSid, tbl1.startTime, tbl1.endTime, tbl2.version
  //        |
  //        |)
  //
  //      """.stripMargin)
  //  }

  /**
    * log_data , log_ref
    *
    * logType
    *
    * @return DataFrame(videoSid, videoName, startTime, endTime,
    *         2_pv, 2_uv, 2_duration, 3_pv, 3_uv, 3_duration, all_pv, all_uv, all_duration)
    */
  //  def getStat(logType: String): DataFrame = {
  //
  //    SqlPattern.joinField = SqlPattern.VIDEOSID_
  //    SqlPattern.timeType = SqlPattern.TIMETYPE.get(logType).get
  //    // version times
  //    // version duration
  //    // filter 2 or 3
  //    // join
  //
  //    // all times
  //    // all duration
  //
  //
  //    val dfVersionTime = getPlayTimesStat.join(getPlayDurStat, "videoSid")
  //
  //    SqlPattern.selectFields = Array(
  //      SqlPattern.VIDEOSID, SqlPattern.VIDEONAME, SqlPattern.VERSION, SqlPattern.STARTTIME, SqlPattern.ENDTIME,
  //      SqlPattern.PV, SqlPattern.UV
  //    ).mkString(",")
  //
  //    val dfVersionDur =
  //      versionDf.filter(s"${SqlPattern.VERSION}= 2")
  //        .join(versionDf.filter(s"${SqlPattern.VERSION} =3")
  //          .select(SqlPattern.VIDEOSID, SqlPattern.VERSION), SqlPattern.VIDEOSID_)
  //
  //    SqlPattern.groupFields = Array(
  //      SqlPattern.VIDEOSID
  //    ).mkString(",")
  //
  //    val allDf = getPlayTimesStat.join(getPlayDurStat, "videoSid")
  //
  //    versionDf.join(allDf, "videoSid")
  //
  //  }

  /**
    *
    * @return DataFrame(videoSid, videoName, startTime, endTime, pv_2, uv_2, duration_2, pv_3, uv_3, duration_3
    */
  //  def withVersionStat: DataFrame = {
  //    SqlPattern.selectFields = Array(
  //      SqlPattern.VIDEOSID, SqlPattern.VIDEONAME, SqlPattern.VERSION, SqlPattern.STARTTIME, SqlPattern.ENDTIME,
  //      SqlPattern.PV, SqlPattern.UV
  //    ).mkString(",")
  //
  //    SqlPattern.groupFields = Array(
  //      SqlPattern.VIDEOSID, SqlPattern.VIDEONAME, SqlPattern.VERSION
  //    ).mkString(",")
  //
  //
  //    val dfTimes = getPlayTimesStat
  //
  //    SqlPattern.selectFields = Array(
  //      SqlPattern.VIDEOSID, SqlPattern.VIDEONAME, SqlPattern.VERSION, SqlPattern.STARTTIME, SqlPattern.ENDTIME,
  //      SqlPattern.DURATION_PER_UV
  //    ).mkString(",")
  //
  //    val dfDuration = getPlayDurStat
  //
  //    val dfVersion = dfTimes.join(dfDuration,
  //      Array(SqlPattern.VIDEOSID_, SqlPattern.VERSION_, SqlPattern.STARTTIME, SqlPattern.ENDTIME))
  //
  //    dfTimes.filter(s"${SqlPattern.VERSION} = 2")
  //      .join(dfTimes.filter(s"${SqlPattern.VERSION} = 3"))
  //      .select(SqlPattern.STARTTIME_,)
  //
  //    dfDuration
  //
  //
  //  }
  //
  //  def withoutVersionStat: DataFrame = {
  //    SqlPattern.selectFields = Array(
  //      SqlPattern.VIDEOSID, SqlPattern.VIDEONAME, SqlPattern.STARTTIME,
  //      SqlPattern.ENDTIME, SqlPattern.PV, SqlPattern.UV
  //    ).mkString(",")
  //
  //    SqlPattern.groupFields = Array(
  //      SqlPattern.VIDEOSID, SqlPattern.VIDEONAME
  //    ).mkString(",")
  //
  //
  //  }

  //  def getPlayTimesStat: DataFrame = {
  //
  //    SqlPattern.events = SqlPattern.PLAY_TIMES_EVENTS.mkString(",")
  //    search(SqlPattern.defineSql)
  //  }
  //
  //  def getPlayDurStat: DataFrame = {
  //
  //    SqlPattern.events = SqlPattern.PLAY_DURATION_EVENTS.mkString(",")
  //    search(SqlPattern.defineSql)
  //  }


}
