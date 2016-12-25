package com.moretv.bi.history

import java.lang.{Long => JLong}
import java.util.Calendar
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object HistoryAndCollectPageStatistics extends BaseClass {
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date, -1)
          val s = sqlContext

          import s.implicits._
          val dfDetail = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.DETAIL)
            .filter($"path".startsWith("home-history-"))
            .select("userId", "path")
            .cache()
          val dfPlayview = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW)
            .filter($"path".startsWith("home-history-"))
            .select("userId", "path")
            .cache()

          val detail_pv = rowArray2Map(dfDetail.groupBy("path").count().collect())
          val detail_uv = rowArray2Map(dfDetail.distinct().groupBy("path").count().collect())
          val playview_vv = rowArray2Map(dfPlayview.groupBy("path").count().collect())
          val playview_uv = rowArray2Map(dfPlayview.distinct().groupBy("path").count().collect())

          val res_df = detail_pv.map(e => {
            val path = e._1
            val pathCH = path2CH(path)
            if (pathCH != "") {
              val pv = e._2
              val uv = detail_uv(path)
              val userPlay_num =
                playview_uv.get(path) match {
                  case Some(t) => t
                  case None => 0
                }
              val vv =
                playview_vv.get(path) match {
                  case Some(t) => t
                  case None => 0
                }
              (pathCH, uv, pv, userPlay_num, vv)
            }
            else null
          }
          ).filter(_ != null).toList
          val res = sc.parallelize(res_df).map(x => (x._1, (x._2, x._3, x._4, x._5))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)).collect()

          if (p.deleteOld) {
            val sqlDelete = "DELETE FROM HistoryAndCollectPageStatistics WHERE day = ?"
            util.delete(sqlDelete, day)
          }

          val sqlInsert = "INSERT INTO HistoryAndCollectPageStatistics(day, page, user_num, access_num, userPlay_num, play_num) VALUES(?,?,?,?,?,?)"
          res.foreach(x =>
            util.insert(sqlInsert, day, x._1, new JLong(x._2._1), new JLong(x._2._2), new JLong(x._2._3), new JLong(x._2._4))
          )

          cal.add(Calendar.DAY_OF_MONTH, -1)
          dfDetail.unpersist()
          dfPlayview.unpersist()
        }
        )
        util.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

  def path2CH(path: String): String = {
    path match {
      case "home-history-history" => "观看历史"
      case "home-history-collect" => "收藏追剧"
      case "home-history-reservation" => "节目预约"
      case _ if (path.startsWith("home-history-mytag-tag") && !(path.contains("-peoplealsolike") || path.contains("-similar") || path.contains("-actor") ||
        path.contains("-tag-tag-") || path.contains("-mytag-mytag-") || path.contains("-mytag-collect") || path.contains("-mytag-history"))) => "标签订阅"
      case _ if (path.startsWith("home-history-subjectcollect-subject") && !(path.contains("-peoplealsolike") || path.contains("-similar") || path.contains("-tag") ||
        path.contains("-actor"))) => "专题收藏"
      case _ => ""
    }
  }

  def rowArray2Map(array: Array[Row]) = {
    array.map(row => (row.getString(0), row.getLong(1))).toMap
  }
}