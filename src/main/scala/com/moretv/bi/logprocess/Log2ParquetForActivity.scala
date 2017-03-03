package com.moretv.bi.logprocess

import java.util.Calendar
import java.text.SimpleDateFormat
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

object Log2ParquetForActivity extends BaseClass{

  private val outputPartionNum = 10
  private val regex = "\\w{1,30}".r

  def main(args: Array[String]) {
//    config.set("spark.executor.memory", "3g").
//      set("spark.cores.max", "100").
//      set("spark.storage.memoryFraction", "0.6")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        val deleteOld = p.deleteOld

        (0 until p.numOfDays).foreach(i => {

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/activity/rawlog/activity.access.log.$inputDate-*"
          val outputPath = s"/log/activity/parquet/$inputDate/"

          val logRdd = sc.textFile(inputPath).map(log => {
            val jsonedLog = LogUtils.logProcess(log)
            if(jsonedLog != null){
              val (logType, jsonStr) = jsonedLog
              if(logType != ""){
                regex findFirstIn logType match {
                  case Some(_) => (logType, jsonStr)
                  case None => null
                }
              }else null
            }else null
          }).filter(_ != null).cache()

          val logTypeSet = logRdd.map(_._1).distinct().collect()

          logTypeSet.foreach(logType => {

            val jsonRdd = logRdd.filter(_._1 == logType).map(_._2)

            if (deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath + logType)
            }
            sqlContext.read.json(jsonRdd).coalesce(outputPartionNum).write.parquet(outputPath + logType)

          })

          cal.add(Calendar.DAY_OF_MONTH, -1)

          logRdd.unpersist()
        })

      }
      case None =>
        throw new RuntimeException("At least need param --startDate.")

    }
  }

}
