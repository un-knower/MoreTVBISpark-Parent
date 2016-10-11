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
              .select("omnibusSid", "omnibusName", "userId", "event", "duration", "contentType")
              .filter("contentType = 'mv'")
              .filter("omnibusSid is not null")
              .filter("omnibusName is not null")
              .filter("duration between '0' and '10800'")
              .cache

            //rdd(omnibusSid,omnibusName,event,duration)
            val rdd = df.map(e => (e.getString(0), e.getString(1), e.getString(3), e.getString(2), e.getLong(4)))
              .cache

            val pvUvRdd = rdd.filter(_._3 == "startplay")
              .map(e => ((e._1, e._2), e._4))

            val durationRdd = rdd.filter(e => {
              e._3 == "userexit" || e._3 == "selfend"
            }).map(e => ((e._1, e._2), e._5))

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
              val pv = pvMap.get(key) match {
                case Some(p) => p
                case None => 0
              }
              val meanDuration = durationMap.get(key) match {
                case Some(p) => p.toFloat / w._2
                case None => 0
              }

              util.insert(insertSql, sqlDate, w._1._1, w._1._2,
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


}
