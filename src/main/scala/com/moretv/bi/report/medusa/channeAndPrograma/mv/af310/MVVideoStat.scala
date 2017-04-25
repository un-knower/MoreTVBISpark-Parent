package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Float => JFloat, Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/30/16.
  */

/**
  * 领域: MV
  * 对象: 视频(不限制路径 contentType = mv)
  * 维度: 天, 视频(id & name)
  * 数据源: play
  * 提取特征: videoSid, userId ,contentType, duration
  * 统计:  pv ,uv, duration
  * 输出: tbl[mv_video_pv_uv_duration](day,video_sid,video_name,uv,pv,duration)
  */
object MVVideoStat extends BaseClass {

  private val dataSource = "play"

  private val tableName = "mv_video_stat"

  private val fields = "day, video_sid, video_name, uv, pv, duration"

  private val insertSql = s"insert into $tableName($fields) values (?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"


  def main(args: Array[String]) {

    ModuleClass.executor(this,args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        // init& util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          val loadPath = s"/log/medusa/parquet/$loadDate/$dataSource"
          println(loadPath)

          //df
          val df =
            DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.PLAY,loadDate)
              .select("videoSid", "videoName", "userId", "event", "duration", "contentType")
              .filter("videoSid is not null")
              .filter("videoName is not null")
              .filter("contentType = 'mv'")
              .filter("duration is not null and duration between '0' and '10800'")
              .cache

          //rdd
          val rdd =
            df.map(e => (e.getString(0), e.getString(1), e.getString(3), e.getString(2), e.getLong(4)))
              .cache

          val pvUvRdd = rdd.filter(_._3 == "startplay")
            .map(e => ((e._1, e._2), e._4))

          val durationRdd = rdd.filter(e => {
            e._3 == "userexit" || e._3 == "selfend"
          })
            .map(e => ((e._1, e._2), e._5))

          //aggregate

          val uvMap = pvUvRdd.distinct.countByKey
          val pvMap = pvUvRdd.countByKey

          val durationMap = durationRdd.reduceByKey(_ + _).collectAsMap()

          //deal with table

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          uvMap.foreach(w => {

            val key = w._1

            val meanDuration = durationMap.get(key) match {
              case Some(p) => p.toFloat / w._2
              case None => 0
            }

            val pv = pvMap.get(w._1) match {
              case Some(p) => p
              case None => 0
            }

            util.insert(
              insertSql, sqlDate, w._1._1, w._1._2, new JLong(w._2), new JLong(pv),
              new JFloat(meanDuration)
            )

          })
        })
      }

      case None => {
        throw new Exception("MVVideoStat fails for not enough params")
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
