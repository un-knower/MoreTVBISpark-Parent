package com.moretv.bi.overview

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by laishun on 15/10/9.
  */
object HotRecommendTagPagePVUVVV extends BaseClass with DateUtil {

  def main(args: Array[String]) {
    config.setAppName("HotRecommendTagPagePVUVVV")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //calculate log whose type is play
        val playRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW)
          .filter("logType='playview'")
          .select("date", "path", "userId")
          .map(e => (e.getString(0), e.getString(1), e.getString(2)))
          .filter(e => judgePath(e._2)).map(e => (getKeys(e._1), e._3))
          .persist(StorageLevel.MEMORY_AND_DISK)

        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()

        val detailRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.DETAIL)
          .select("date", "path", "userId")
          .map(e => (e.getString(0), e.getString(1), e.getString(2)))
          .filter(e => judgePath(e._2)).map(e => (getKeys(e._1, "detail"), e._3))
          .persist(StorageLevel.MEMORY_AND_DISK)

        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()

        //save date
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from hotrecommendTagPagePVUVVV where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO hotrecommendTagPagePVUVVV(year,month,day,type,user_num,access_num) VALUES(?,?,?,?,?,?)"
        userNum_play.foreach(x => {
          util.insert(sql, new Integer(x._1._1), new Integer(x._1._2), x._1._3, x._1._4, new Integer(x._2.toInt), new Integer(accessNum_play(x._1).toInt))
        })

        userNum_detail.foreach(x => {
          util.insert(sql, new Integer(x._1._1), new Integer(x._1._2), x._1._3, x._1._4, new Integer(x._2.toInt), new Integer(accessNum_detail(x._1).toInt))
        })

        playRDD.unpersist()
        detailRDD.unpersist()
        df.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def judgePath(path: String) = {
    val reg = "home-hotrecommend(-\\d+-\\d+)?-(tag-)+(.+?)".r
    val pattern = reg findFirstMatchIn path
    val res = pattern match {
      case Some(x) => true
      case None => false
    }
    res
  }

  def getKeys(date: String, logType: String = "play") = {
    //obtain time
    val year = date.substring(0, 4)
    val month = date.substring(5, 7).toInt

    (year, month, date, logType)
  }
}
