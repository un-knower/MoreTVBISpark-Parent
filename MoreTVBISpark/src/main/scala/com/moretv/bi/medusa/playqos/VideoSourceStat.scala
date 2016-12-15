package com.moretv.bi.medusa.playqos

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 9/7/16.
  */


/**
  *  领域： 播放源
  *  对象： 播放源
  *  维度： 天，类型， 源
  *  统计： pv, uv
  *  输出： tbl[video_source_play_stat](day,contenttype,source,pv,uv)
  */
object VideoSourceStat extends BaseClass {

  private val tableName = "video_source_play_stat"

  private val fields = "day,contenttype,source,pv,uv"

  private val insertSql = s"insert $tableName($fields)values(?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(VideoSourceStat, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        var readPath = ""
        (0 until p.numOfDays).foreach(i => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          readPath = s"/log/medusa/parquet/$loadDate/playqos"

          println(readPath)

          val rdd = sqlContext.read.parquet(readPath)
            .select("date", "jsonLog", "userId")
            .filter(s"date='$sqlDate'")
            .map(e => (e.getString(0), e.getString(1), e.getString(2)))


          val tmpRdd = rdd
            .flatMap(e => sourceMap(e._1, e._2, e._3))
            .filter(_._1 != null)
            .filter(_._2 != null)
            .filter(_._3 != null)
            .map(e => ((e._1, e._2, e._3), e._4))


          val uvMap = tmpRdd.distinct.countByKey

          val pvMap = tmpRdd.countByKey

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          uvMap.foreach(w => {

            val key = w._1
            val uv = new JLong(w._2)
            val pv = pvMap.get(key) match {
              case Some(p) => new JLong(p)
              case None => new JLong(0)
            }
            val date = w._1._1
            val contentType = w._1._2
            val videoSource = w._1._3

            util.insert(insertSql, date, contentType, videoSource, pv, uv)
          })

        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }


  }


  /**
    *
    * @param day
    * @param jsonLog json字符串
    * @return (userId, day, contentType, videoSource)
    */
  def sourceMap(day: String, jsonLog: String, userId: String) = {

    val res = new ListBuffer[(String, String, String, String)]()

    try {
      val jsObj = new JSONObject(jsonLog)

      val contentType = jsObj.optString("contentType") match {
        case "sports" | "mv" | "hot" | "xiqu" => "short"

        case "movie" | "tv" | "jilu" | "kids" | "zongyi" | "comic" => "long"

        case _ => "others"
      }


      val playqosArr = jsObj.optJSONArray("playqos")

      if (playqosArr != null) {

        (0 until playqosArr.length).foreach(i => {

          val obj = playqosArr.optJSONObject(i)

          val videoSource = obj.optString("videoSource")

          res.+=((day, contentType, videoSource, userId))
        })
      }
    }
    catch {
      case ex: Exception => {
        res.+=((day, null, null, userId))
      }
    }
    res.toList
  }

}
