package com.moretv.bi.content

import com.moretv.bi.constant.LogType._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DFUtil, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/4/18.
 */
object VodAndLiveUVMonth extends BaseClass{


  def main(args: Array[String]) {
    ModuleClass.executor(VodAndLiveUVMonth,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        var logType = PLAYVIEW
        val sqlPlay = "select distinct userId from log_data"
        val dfPlay = DFUtil.getDFByDateWithSql(sqlPlay,logType,p.startDate,p.numOfDays).cache()

        val playUserNum = dfPlay.count()
        println(s"playUserNum:$playUserNum")

        logType = LIVE
        val sqlLive = "select distinct userId from log_data"
        val dfLive = DFUtil.getDFByDateWithSql(sqlLive,logType,p.startDate,p.numOfDays).cache()

        val liveUserNum = dfLive.count()
        println(s"liveUserNum:$liveUserNum")

        val userIds = dfLive unionAll dfPlay
        val totalUserNum = userIds.distinct().count()
        println(s"totalUserNum:$totalUserNum")
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

}