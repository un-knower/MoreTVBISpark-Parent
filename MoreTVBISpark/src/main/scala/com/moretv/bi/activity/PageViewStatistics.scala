package com.moretv.bi.activity

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{LogUtils, SparkSetting}
import com.moretv.bi.util.FileUtils._
import java.util.Date

import org.apache.spark.SparkContext
import com.moretv.bi.constant.Activity._

/**
 * Created by Will on 2015/7/17.
 */
object PageViewStatistics extends BaseClass{

  val SEPARATOR = ","

  def main(args: Array[String]) {
    ModuleClass.executor(PageViewStatistics,args)
  }
  override def execute(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)

    val logRdd = sc.textFile(inputPath).map(log => LogUtils.log2json(log)).
      filter(json => json != null && json.opt(ACTIVITY_ID) != null)
    val rdd = logRdd.filter(json => (json.optString(LOG_TYPE) == PAGEVIEW && json.optString(PAGE) == INDEX)).
      map(json => (json.optString(FROM),json.optString(USER_ID))).cache()
    val pv = rdd.countByKey()
    val uv = rdd.distinct().countByKey()
    withCsvWriterOld(outputPath){
      out => {
        out.println(new Date)
        pv.foreach(e => {
          val key = e._1
          val userNum = uv.get(key).get
          out.println(key+SEPARATOR+userNum+SEPARATOR+e._2)
        })
      }
    }
    rdd.unpersist()
  }
}
