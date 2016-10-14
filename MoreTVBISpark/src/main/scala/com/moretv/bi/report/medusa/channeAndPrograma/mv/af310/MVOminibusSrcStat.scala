package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/21/16.
  */

/**
  * 领域: MV [Medusa 3.1.0 及以上版本]
  * 对象: 精选集
  * 维度: 来源(入口), 天, 精选集(id & name)
  * 统计: pv, uv ,mean_duration
  * 数据源: play (注 medusa3.1.0 有新增字段[omnibusSid,omnibusName])
  * 提取特征: omnibusSid, omnibusName, userId, pathMain, duration
  * 输出: tbl[mv_ominibus_src_stat](day,entrance,ominibus_sid,ominibus_name,uv,pv,duration)
  */
object MVOminibusSrcStat extends BaseClass {

  private val dataSource = "play"

  private val tableName = "mv_ominibus_src_stat"

  private val fields = "day, entrance, ominibus_sid, ominibus_name, uv, pv, duration"

  private val insertSql = s"insert into $tableName ($fields) values(?,?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  private val filterPathRegex = (
    "(mv\\*mvRecommendHomePage|mv\\*function\\*site_mvsubject|" +
      "mv\\*mvCategoryHomePage\\*site|home\\*recommendation|search|mv\\*mineHomePage\\*site_collect)"
    ).r


  def main(args: Array[String]) {

    ModuleClass.executor(MVOminibusSrcStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        // init & util
        val util = new DBOperationUtils("medusa")
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
          //check schema
          if (sqlContext.read.parquet(loadPath).schema.simpleString contains ("omnibusSid")) {

            //df
            val df = sqlContext.read.parquet(loadPath)
              .select("pathMain", "omnibusSid", "omnibusName", "userId", "event", "duration", "contentType")
              .filter("contentType = 'mv'")
              .filter("omnibusSid is not null")
              .filter("omnibusName is not null")
              .filter("duration between '0' and '10800'")
              .cache

            //rdd(pathMain, ominibusSid, ominibusName, event, userId, duration)
            val rdd = df.map(e => (e.getString(0), e.getString(1), e.getString(2), e.getString(4),
              e.getString(3), e.getLong(5))).cache

            val pvUvRdd = rdd.filter(_._4 == "startplay")
              .map(e => (
                (filterEntrance(e._1), e._2, e._3), e._5)
              )
              .filter(_._1._1 != null)

            val durationRdd = rdd.filter(e => (e._4 == "userexit" || e._4 == "selfend"))
              .map(e => (
                ((filterEntrance(e._1), e._2, e._3), e._6))
              )
              .filter(_._1._1 != null)

            //aggregate
            val uvMap = pvUvRdd.distinct.countByKey

            val pvMap = pvUvRdd.countByKey

            val durationMap = durationRdd.reduceByKey(_ + _).collectAsMap

            //deal with table
            if (p.deleteOld) {
              util.delete(deleteSql, sqlDate)
            }

            uvMap.foreach(w => {
              val key = w._1
              val pv = pvMap.get(key) match {
                case Some(p) => p
                case None => 0
              }
              val meanDuration = durationMap.get(key) match {
                case Some(p) => p.toFloat / w._2
                case None => 0
              }

              util.insert(insertSql, sqlDate, w._1._1, w._1._2, w._1._3,
                new JLong(w._2), new JLong(pv), new JDouble(meanDuration))
            })

          }

        })
      }
      case None => {
        throw new Exception("MVOminibusPvUv fails for not enough params")
      }
    }
  }

  /**
    *
    * @param field 用pathMain来确定父级入口
    * @return entrance 字段
    */
  def filterEntrance(field: String): String = {

    filterPathRegex findFirstMatchIn field match {

      case Some(p) => {

        p.group(1) match {

          case "mv*mvRecommendHomePage" => "音乐首页推荐"

          case "mv*function*site_mvsubject" => "精选集入口"

          case "mv*mvCategoryHomePage*site" => "音乐下的某分类"

          case "home*recommendation" => "launcher推荐"

          case "search" => "搜索"

          case "mv*mineHomePage*site_collect" => "音乐收藏"

          case _ => null

        }

      }

      case None => null
    }
  }

}
