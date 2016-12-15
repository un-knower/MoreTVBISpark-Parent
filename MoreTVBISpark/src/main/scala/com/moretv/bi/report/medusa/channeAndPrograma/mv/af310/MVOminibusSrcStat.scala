package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.channeAndPrograma.mv.MVRecommendPlay._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, LiveCodeToNameUtils, ParamsParseUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

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
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        sqlContext.udf.register("getSidFromPath", getSidFromPath _)

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

          val df = makeDataFrame(loadPath)


          //rdd(pathMain, ominibusSid, userId, duration, event)
          val rdd = df.map(e => (e.getString(0), e.getString(1), e.getString(2), e.getLong(3), e.getString(4)))
            .filter(_._2 != null)
            .cache

          val pvUvRdd = rdd.filter(_._5 == "startplay")
            .map(e => (
              (filterEntrance(e._1), e._2), e._3)
            )
            .filter(_._1._1 != null)

          val durationRdd = rdd.filter(e => (e._5 == "userexit" || e._5 == "selfend"))
            .map(e => (
              ((filterEntrance(e._1), e._2), e._4))
            )
            .filter(_._1._1 != null)

          //aggregate
          val uvMap = pvUvRdd.distinct.countByKey

          val pvMap = pvUvRdd.countByKey

          val durationMap = durationRdd.reduceByKey(_ + _).collectAsMap

          val nameRefs = scala.collection.mutable.HashMap.empty[String, String]


          sqlContext.read.parquet(loadPath).registerTempTable("log_ref")
          sqlContext.sql(
            """
              |select omnibusSid, omnibusName from log_ref
              |  where omnibusSid is not null and omnibusName is not null
            """.stripMargin)
            .distinct
            .collect
            .foreach(w => {
              nameRefs += (w.getString(0) -> w.getString(1))
            })

          //deal with table
          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          uvMap.foreach(w => {
            val key = w._1

            val source = w._1._1

            if (nameRefs.contains(w._1._2)) {

              val omnibusSid = w._1._2
              var omnibusName = nameRefs.getOrElse(omnibusSid, LiveCodeToNameUtils.getMVSubjectName(omnibusSid))

              val uv = new JLong(w._2)
              val pv = pvMap.get(key) match {
                case Some(p) => p
                case None => 0
              }
              val meanDuration = durationMap.get(key) match {
                case Some(p) => p.toFloat / w._2
                case None => 0
              }

              util.insert(insertSql, sqlDate, source, omnibusSid, omnibusName,
                uv, new JLong(pv), new JDouble(meanDuration))

            }

          })


        }

        )
      }
      case None => {
        throw new Exception("MVOminibusSrcStat fails for not enough params")
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

  /**
    *
    * @param loadPath
    * @return dataFrame(pathMain,omnibusSid,userId,duration,event)
    */
  def makeDataFrame(loadPath: String): DataFrame = {

    sqlContext.udf.register("getSidFromPath", getSidFromPath _)

    sqlContext.read.parquet(loadPath)
      .filter("pathMain is not null")
      .filter("duration between 0 and 10800")
      .registerTempTable("log_data")

    val df = sqlContext.sql(
      """
        |select pathMain, case when omnibusSid is null then getSidFromPath(pathMain) else omnibusSid end as omnibusSid,
        |  userId, duration, event,videoSid from log_data
      """.stripMargin)
      .filter("omnibusSid != videoSid")
      .select("pathMain", "omnibusSid", "userId", "duration", "event")
    df

    df
  }

  def getSidFromPath(path: String) = {
    if (path != null && path.contains("*")) {
      val splitPath = path.split("\\*")
      val lastInfo = splitPath(splitPath.length - 1)
      if (lastInfo.length == 12) {
        lastInfo
      } else null
    } else null
  }


}
