package com.moretv.bi.common


import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import java.lang.{Long => JLong}

/**
  * Created by laishun on 15/10/9.
  */
object CommonTotalTime extends BaseClass with DateUtil {
  def main(args: Array[String]) {
    config.setAppName("CommonTotalTime")
    ModuleClass.executor(CommonTotalTime, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val resultRDD = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.INTERVIEW)
          .filter("event = 'exit' and duration < 72000 and duration >0 ")
          .select("date", "path", "userId", "duration")
          .map(e => (e.getString(0), e.getString(1), e.getString(2), e.getInt(3)))
          .filter(e => judgePath(e._2))
          .map(e => (getKeys(e._1, e._2), e._3, e._4))
          .persist(StorageLevel.MEMORY_AND_DISK)

        val userNum = resultRDD.map(e => (e._1, e._2)).distinct().countByKey()
        val time = resultRDD.map(e => (e._1, e._3.toLong)).reduceByKey((x, y) => x + y).collect().toMap

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from commonTotalTime where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO commonTotalTime(year,month,day,weekstart_end,module,user_num,time) VALUES(?,?,?,?,?,?,?)"
        userNum.foreach(x => {
          util.insert(sql, new Integer(x._1._1), new Integer(x._1._2), x._1._3, x._1._4, x._1._5, new JLong(x._2), new JLong(time(x._1)))
        })
        resultRDD.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def judgePath(path: String) = {
    val subject = List("movie", "tv", "zongyi", "comic", "kids", "jilu", "mv", "hot", "xiqu", "sports", "kids_home", "search", "history")
    val array = path.split("-")
    if (array.length == 2 && subject.contains(array(1)) && "home".equalsIgnoreCase(array(0))) {
      true
    } else {
      false
    }
  }

  def getKeys(date: String, path: String) = {
    val year = date.substring(0, 4)
    val month = date.substring(5, 7).toInt
    val week = getWeekStartToEnd(date)

    val array = path.split("-")
    val module = array(1)

    (year, month, date, week, module)
  }
}
