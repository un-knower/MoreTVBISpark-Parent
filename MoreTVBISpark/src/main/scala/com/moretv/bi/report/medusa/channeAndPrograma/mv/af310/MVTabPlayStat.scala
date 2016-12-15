package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Float => JFloat, Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.channeAndPrograma.mv.PlayPathMatch
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

import scala.collection.mutable.ListBuffer


/**
  * Created by witnes on 9/20/16.
  */

/**
  * 领域: mv
  * 对象: 列表
  * 数据源: play
  * 维度: 天, 入口(多级), 视频
  * 提取特征: pathMain , userId , duration
  * 过滤条件: 收藏,推荐,电台, 榜单, 分类, 搜索,歌手, 舞蹈,精选集, 演唱会
  * 统计: pv ,uv, mean_duration
  * 输出: tbl[mv_tab_play_stat](day, tabname, entrance, pv, uv, mean_duration)
  *
  */

object MVTabPlayStat extends BaseClass {

  private val dataSource = "play"

  private val tableName = "mv_tab_play_stat"

  private val fields = "day,tabname,entrance,uv,pv,mean_duration"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"


  def main(args: Array[String]) {

    ModuleClass.executor(MVTabPlayStat, args)

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
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          val loadPath = s"/log/medusa/parquet/$loadDate/$dataSource"
          println(loadPath)


          val df = sqlContext.read.parquet(loadPath)
            .select("pathMain", "event", "userId", "duration","contentType")
            .filter("contentType ='mv'")
            .filter("pathMain is not null")
            .filter("duration is not null and duration < 10800 and duration >= 0 ")
            .cache


          val rdd =
            df.flatMap(
              e =>
                PlayPathMatch.mvPathMatch(
                  e.getString(0), e.getString(1), e.getString(2), e.getLong(3)
                )
            )
              .filter(_._1 != null)
              .filter(_._2 != null)
              .cache


          //pvuvRdd(home_tab, entrance, userId) with (event = startplay)

          val pvuvRdd =
            rdd.filter(_._3 == "startplay")
              .map(e => ((e._1, e._2), e._4))


          //durationRdd(home_tab, entrance, duration) with (event = userexit|selfend)

          val uvRddForDuration = rdd.filter(e => {
            e._3 == "userexit" || e._3 == "selfend"
          })
            .map(e => ((e._1, e._2), e._4))

          val durationRdd =
            rdd.filter(e => {
              e._3 == "userexit" || e._3 == "selfend"
            })
              .map(e => ((e._1, e._2), e._5))


          //aggregate

          val uvMap = pvuvRdd.distinct.countByKey

          val pvMap = pvuvRdd.countByKey

          val uvMap2 = uvRddForDuration.distinct.countByKey

          val durationMap = durationRdd.reduceByKey(_ + _).collectAsMap()


          // deal with table

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          uvMap.foreach(w => {

            val key = w._1

            val uv2 = uvMap2.get(key) match {

              case Some(p) => p
              case None => 0

            }

            val meanDuration = durationMap.get(key) match {

              case Some(p) => p.toFloat / uv2
              case None => 0

            }

            val pv = pvMap.get(w._1) match {

              case Some(p) => p
              case None => 0

            }

            util.insert(
              insertSql, sqlDate, w._1._1, w._1._2, new JLong(w._2), new JLong(pv), new JFloat(meanDuration)
            )


          })

        })
      }
      case None => {

        throw new Exception("MVTabPlayStat fails")

      }
    }
  }


}
