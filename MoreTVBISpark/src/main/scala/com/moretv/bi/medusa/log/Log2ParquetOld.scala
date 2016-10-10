package com.moretv.bi.medusa.log

import java.util.Calendar

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by Will on 2015/8/20.
 */
object Log2ParquetOld extends BaseClass{

  private val repartitionNum = 600
  private val outputPartitionNum = 40
  private val regex = "^\\w{3,30}$".r

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "15g").
      set("spark.cores.max", "200").
      set("spark.executor.cores", "5").
      set("spark.storage.memoryFraction", "0.6")
    ModuleClass.executor(Log2ParquetOld,args)
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

          val logRdd = sc.textFile(inputPath).repartition(repartitionNum).map(log => {
            val jsonLog = LogUtils.logProcessMedusa(log)
            if(jsonLog != null){
              val (logType, userId, jsonStr) = jsonLog
              if (logType != "") {
                regex findFirstIn logType match {
                  case Some(_) => {
                    if(!DevMacUtils.userIdFilter(userId)) (logType,jsonStr) else null
                  }
                  case None => null
                }
              } else null
            } else null

          }).filter(_ != null).persist(StorageLevel.MEMORY_AND_DISK_SER)


          val logTypeSet = if(p.logTypes.nonEmpty){
              p.logTypes.split(",")
            }else{
            logRdd.map(_._1).distinct().collect()
            }


          logTypeSet.foreach(logType => {
            val jsonRdd = logRdd.filter(logType == _._1).map(_._2)

            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath + logType)
            }
            sqlContext.read.json(jsonRdd).coalesce(outputPartitionNum).write.parquet(outputPath + logType)
          })

          logRdd.unpersist()
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
