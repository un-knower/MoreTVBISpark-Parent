package com.moretv.bi.temp.medusa

import java.util.Calendar

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/8/20.
 */
object Log2Parquet extends SparkSetting{

  private val outputPartitionNum = 40

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "5g").
          set("spark.cores.max", "200").
          set("spark.executor.cores", "5").
          set("spark.storage.memoryFraction", "0.6")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/medusa/rawlog/$inputDate/*"
          val outputPath = s"/log/medusa/parquet/$inputDate/"
          val logTypes = p.logTypes

          val logRdd = sc.textFile(inputPath)

          logTypes.split(",").foreach(inputLogType => {
            val jsonRdd = logRdd.map(log => {
              if(log.contains(s"logType=$inputLogType")){
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
