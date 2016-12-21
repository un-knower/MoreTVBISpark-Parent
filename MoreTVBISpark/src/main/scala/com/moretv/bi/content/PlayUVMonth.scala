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
object PlayUVMonth extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val logType = PLAYVIEW
        val sql = "select distinct contentType,userId from log_data"
        val result = DFUtil.getDFByDateWithSql(sql,logType,p.startDate,p.numOfDays).map(row => row.getString(0)).countByValue()

        result.foreach(row => println(s"${row._1},${row._2}"))
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

}
