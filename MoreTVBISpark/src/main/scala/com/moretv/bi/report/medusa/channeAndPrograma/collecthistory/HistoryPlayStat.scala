package com.moretv.bi.report.medusa.channeAndPrograma.collecthistory

import java.util.Calendar
import java.lang.{Long => JLong,Float => JFloat}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by witnes on 11/7/16.
  */

/**
  * 历史播放总体 人数 次数
  */
object HistoryPlayStat extends BaseClass {

  private val tableName = "history_intervals_play_stat"

  private val fields = "interval_type,intervals,vv,uv,duration"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where  interval_type = ? and intervals = ?"

  def main(args: Array[String]) {
    ModuleClass.executor(HistoryPlayStat, args)
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

          val loadPath = s"/log/medusaAndMoretvMerger/$loadDate/playview"

          val df = sqlContext.read.parquet(loadPath)
            .filter(s"date between '$sqlDate' and '$sqlDate'")
            .filter("pathMain like '%history%' or pathMain like '%观看历史%'")
            .select("date", "userId", "event", "duration")

          val vv = df.filter("event in ('startplay','playview')")
            .count

          val uv = df.filter("event in ('startplay','playview')")
            .distinct.count

          val duration = df.filter("event not in ('startplay','playview')")
            .filter("duration between 1 and 10800")
            .select("duration")
            .map(e => e.getLong(0))
            .reduce(_ + _)

          val durationPerUv = duration / uv

          if (p.deleteOld) {
            util.delete(deleteSql, "day", sqlDate)
          }

          util.insert(insertSql, "day", sqlDate, new JLong(vv), new JLong(uv), new JFloat(durationPerUv))
        })
      }
      case None => {

      }

    }
  }
}
