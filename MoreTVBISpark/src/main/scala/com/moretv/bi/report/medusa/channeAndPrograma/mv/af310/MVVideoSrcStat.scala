package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Float => JFloat, Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.channeAndPrograma.mv.PlayPathMatch
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 9/20/16.
  */

/**
  * 领域: MV
  * 对象: 视频 (限制路径)
  * 维度: 天, 入口(多级), 视频(id & name)
  * 数据源: play
  * 提取特征: pathMain , videoSid, userId , duration
  * 过滤条件: 收藏,推荐,推荐-电台, 榜单, 分类, 搜索,歌手, 舞蹈,精选集, 演唱会
  * 统计: entrance, pv ,uv, duration
  * 输出: tbl[mv_video_src_stat](day,entrance,video_sid,video_name,uv,pv,duration)
  */

object MVVideoSrcStat extends BaseClass {

  private val dataSource = "play"

  private val tableName = "mv_video_src_stat"

  private val fields = "day,entrance,video_sid,video_name,uv,pv,duration"

  private val insertSql = s"insert into $tableName($fields)values(?,?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day =?"


  def main(args: Array[String]) {

    ModuleClass.executor(MVVideoSrcStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        // init & util
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
            sqlContext.read.parquet(loadPath)
              .select("pathMain", "videoSid", "videoName", "event", "userId", "duration", "contentType")
              .filter("contentType ='mv'")
              .filter("pathMain is not null")
              .filter("videoSid is not null")
              .filter("videoName is not null")
              .filter("duration is not null and duration between '0' and '10800'")
              .cache


          //rdd(entrance, videoSid, videoName, event, userId, duration)

          val rdd =
            df.flatMap(
              e =>
                PlayPathMatch.mvPathMatch(
                  e.getString(0), e.getString(1), e.getString(2), e.getString(3), e.getString(4), e.getLong(5)
                )
            )
              .filter(_._1 != null)
              .cache

          //pvuvRdd((entrance, videoSid, videoName), userId)

          val pvuvRdd = rdd.filter(_._4 == "startplay")
            .map(e => ((e._1, e._2, e._3), e._5))

          //durationRdd((entrance,videoSid, videoName), duration)

          val durationRdd = rdd.filter(e => {
            e._4 == "userexit" || e._4 == "selfend"
          })
            .map(e => ((e._1, e._2, e._3), e._6))

          //aggregate

          val uvMap = pvuvRdd.distinct.countByKey

          val pvMap = pvuvRdd.countByKey

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
              insertSql, sqlDate, w._1._1, w._1._2, w._1._3, new JLong(w._2), new JLong(pv),
              new JFloat(meanDuration)
            )

          })

        })
      }
      case None => {
        throw new Exception("MVPlayVideoRankStat fails")
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