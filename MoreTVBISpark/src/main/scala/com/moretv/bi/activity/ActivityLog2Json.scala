package com.moretv.bi.activity

import com.moretv.bi.constant.Activity._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{LogUtils, SparkSetting}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext

/**
 * Created by Will on 2015/8/18.
 */
object ActivityLog2Json extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(ActivityLog2Json,args)
  }
  override def execute(args: Array[String]) {
    val inputDate = args(0)
    val inputPath = "/log/activity/activity_"+inputDate+"*"
    val outputPath = "/log/json/activity/"+inputDate+"/"

    val logRdd = sc.textFile(inputPath).map(logProcess).filter(_ != null).cache()
    val logTypeSet = logRdd.map(_._1).distinct().collect()
    logTypeSet.foreach(logType => {
      logRdd.filter(_._1 == logType).map(_._2).coalesce(1).
        saveAsTextFile(outputPath+logType,classOf[BZip2Codec])
    })
    logRdd.unpersist()
  }

  def logProcess(log:String) = {
    val json = LogUtils.log2json(log)

    if(json != null){
      if(json.opt(ACTIVITY_ID) != null){
        val logType = json.opt(LOG_TYPE)
        (logType,json)
      }else null
    }else null
  }
}
