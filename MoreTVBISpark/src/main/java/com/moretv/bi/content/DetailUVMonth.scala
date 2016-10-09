package com.moretv.bi.content


import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.moretv.bi.util.DFUtil
import com.moretv.bi.constant.LogType._

/**
 * Created by Will on 2015/4/18.
 */
object DetailUVMonth extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(DetailUVMonth,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val logType = DETAIL
        val sql = "select distinct contentType,userId from log_data"
        val result = DFUtil.getDFByDateWithSql(sql,logType,p.startDate,p.numOfDays).map(row => row.getString(0)).countByValue()

        result.foreach(row => println(s"${row._1},${row._2}"))
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

}
