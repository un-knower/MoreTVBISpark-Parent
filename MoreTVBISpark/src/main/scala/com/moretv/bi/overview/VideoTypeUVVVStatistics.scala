package com.moretv.bi.overview

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
  * Created by laishun on 15/10/9.
  */
object VideoTypeUVVVStatistics extends BaseClass with DateUtil {
  def main(args: Array[String]) {
    config.setAppName("VideoTypeUVVVStatistics")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val playRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW)
          .filter("apkVersion >'2.4.5'")
          .select("date", "path", "userId")
          .map(e => (e.getString(0), e.getString(1), e.getString(2)))
          .filter(e => judgePath(e._2, "play"))
          .map(e => (getKeys(e._1, e._2), e._3))
          .persist(StorageLevel.MEMORY_AND_DISK)

        val playDurationRDD = playRDD.map(e => ((e._1._1, e._1._2, e._1._3, "total", e._1._5), e._2)).union(playRDD)
          .countByKey()


        val liveRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.LIVE)
          .select("date", "path", "userId")
          .map(e => (e.getString(0), e.getString(1), e.getString(2)))
          .filter(e => judgePath(e._2, "live"))
          .map(e => (getKeys(e._1, e._2), e._3))
          .persist(StorageLevel.MEMORY_AND_DISK)


        val liveDurationRDD = liveRDD.map(e => ((e._1._1, e._1._2, e._1._3, "total", e._1._5), e._2))
          .union(liveRDD).countByKey()


        val pastRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.LIVE).
          filter("logType='live' and liveType='past'").select("date", "path", "userId").map(e => (e
          .getString(0), e.getString(1), e.getString(2))).
          filter(e => judgePath(e._2, "live")).map(e => (getKeys(e._1, e._2), e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val pastDurationRDD = pastRDD.map(e => ((e._1._1, e._1._2, e._1._3, "total", e._1._5), e._2)).union(pastRDD).countByKey()

        //save date
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from video_type_statistics where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO video_type_statistics(year,month,day,access_source,type,live,watchpast,vod) VALUES(?,?,?,?,?,?,?,?)"
        playDurationRDD.foreach(x => {
          util.insert(sql, new Integer(x._1._1), new Integer(x._1._2), x._1._3, x._1._4, x._1._5,
            new Integer(liveDurationRDD.getOrElse(x._1, 0L).toInt), new Integer(pastDurationRDD.getOrElse(x._1, 0L).toInt), new Integer(x._2.toInt))
        })

        playRDD.unpersist()
        liveRDD.unpersist()
        pastRDD.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def judgePath(path: String, flag: String) = {
    val reg = if (flag == "play")
      "(home|thirdparty_\\d{1})".r
    else
      "(home|thirdparty_\\d{1})-(live|TVlive)".r

    val pattern = reg findFirstMatchIn path
    val res = pattern match {
      case Some(x) => true
      case None => false
    }
    res
  }

  def getKeys(date: String, path: String) = {
    //obtain time
    val year = date.substring(0, 4)
    val month = date.substring(5, 7).toInt

    val array = path.split("-")
    var access_source = array(0)
    val index = access_source.indexOf("_")
    if (index > 0) access_source = access_source.substring(0, index)

    (year, month, date, access_source, "uv")
  }
}
