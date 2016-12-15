package com.moretv.bi.medusa.log

import java.util.Calendar

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/8/20.
 */
object Log2Parquet extends BaseClass{

  private val outputPartitionNum = 40

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.cores.max", "200").
      set("spark.executor.cores", "5").
      set("spark.storage.memoryFraction", "0.1")
    ModuleClass.executor(Log2Parquet,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/medusa/rawlog/$inputDate/*"
          val outputPath = s"/log/medusa/parquet/$inputDate/"
          val logTypes = p.logTypes

          val logRdd = sc.textFile(inputPath)

          logTypes.split(",").par.foreach(inputLogType => {
            val jsonRdd = logRdd.map(log => {
              if(log.contains(s"logType=$inputLogType") && !UserBlackListUtil.isBlack(log)){
                val jsonLog = LogUtils.logProcessMedusa(log)
                if(jsonLog != null){
                  val (logType, userId, jsonStr) = jsonLog
                  if (logType == inputLogType) {
                    if(!DevMacUtils.userIdFilter(userId)) jsonStr else null
                  }else null
                } else null
              } else null
            }).filter(_ != null)
            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath + inputLogType)
            }
            sqlContext.read.json(jsonRdd).coalesce(outputPartitionNum).write.parquet(outputPath + inputLogType)
          })

          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

}
