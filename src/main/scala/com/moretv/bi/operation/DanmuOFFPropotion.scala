package com.moretv.bi.operation

import java.lang.{Double => JDouble}
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Administrator on 2016/3/1.
  */
object DanmuOffPropotion extends BaseClass {
  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date, -1)

          val df = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.DANMUSWITCH, date)
            .filter("event = 'off'")
            .select("userId")
            .cache()

          val df_live = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.PLAYVIEW, date)
            .filter("path = 'home-hot-danmuzhuanqu'")
            .select("userId")
            .cache()

          //val pv = df.count()
          val uv = df.distinct().count()
          val live_userNum = df_live.distinct().count()
          val propotion: Double = uv.toDouble / live_userNum

          if (p.deleteOld) {
            val sqlDelete = "Delete from danmuOffPropotion where day = ?"
            util.delete(sqlDelete, day)
          }
          val sqlInsert = "INSERT INTO danmuOffPropotion(day,propotion) VALUES(?,?)"
          util.insert(sqlInsert, day, new JDouble(propotion))
          df.unpersist()
          df_live.unpersist()
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
        util.destory()
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }
}