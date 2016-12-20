package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Double => JDouble, Long => JLong}
import java.util
import java.util.Calendar

import com.moretv.bi.report.medusa.channeAndPrograma.mv.MVRecommendPlay._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, LiveCodeToNameUtils, ParamsParseUtil}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by witnes on 9/21/16.
  */

/**
  * 领域: MV [Medusa 3.1.0 及以上版本]
  * 对象: 精选集
  * 维度: 天
  * 数据源: play (注 medusa3.1.0 有新增字段)
  * 提取特征: omnibusSid, omnibusName, userId, pathMain,duration
  * 统计: pv, uv ,mean_duration
  */
object MVOminibusStat extends BaseClass {

  private val dataSource = "play"

  private val tableName = "mv_ominibus_stat"

  private val insertSql =
    s"insert into $tableName(day,ominibus_sid,ominibus_name,uv,pv,duration)values(?,?,?,?,?,?)"

  private val deleteSql =
    s"delete from $tableName where day = ?"

  def main(args: Array[String]) {

    ModuleClass.executor(MVOminibusStat, args)

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

          val df = makeDataFrame(loadPath)

          //rdd(omnibusSid,userId,duration,event)
          val rdd = df.map(e => (e.getString(0), e.getString(1), e.getLong(2), e.getString(3)))
            .filter(_._1 != null)
            .cache

          val pvUvRdd = rdd.filter(_._4 == "startplay")
            .map(e => (e._1, e._2))

          val durationRdd = rdd.filter(e => {
            e._4 == "userexit" || e._4 == "selfend"
          }).map(e => (e._1, e._3))

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

            if (nameRefs.contains(key)) {
              val ominibusSid = w._1

              var ominibusName = nameRefs.getOrElse(ominibusSid, LiveCodeToNameUtils.getMVSubjectName(ominibusSid))

              val uv = new JLong(w._2)

              val pv = pvMap.get(key) match {
                case Some(p) => p
                case None => 0
              }
              val meanDuration = durationMap.get(key) match {
                case Some(p) => p.toFloat / w._2
                case None => 0
              }

              util.insert(insertSql, sqlDate, ominibusSid, ominibusName, uv, new JLong(pv), new JDouble(meanDuration))
            }

          })

        })
      }
      case None => {
        throw new Exception("MVOminibusPvUv fails for not enough params")
      }
    }
  }

  /**
    *
    * @param loadPath
    * @return dataFrame(omnibusid,userId,duration,event)
    */
  def makeDataFrame(loadPath: String): DataFrame = {

    sqlContext.udf.register("getSidFromPath", getSidFromPath _)

    sqlContext.read.parquet(loadPath)
      .filter("pathMain is not null")
      .filter("duration between 0 and 10800")
      .registerTempTable("log_data")

    val df = sqlContext.sql(
      """
        |select case when omnibusSid is null then getSidFromPath(pathMain) else omnibusSid end as omnibusSid,videoSid,
        |  userId, duration, event from log_data
      """.stripMargin)
      .filter("omnibusSid != videoSid")
      .select("omnibusSid", "userId", "duration", "event")

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
