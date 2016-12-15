package com.moretv.bi.metis.log

import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
 * Created by Will on 2016/05/18.
 */
object Log2Parquet extends BaseClass{

  private val repartitionNum = 100
  private val outputPartionNum = 10
  private val regex = "^\\w{1,30}$".r

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "2g").
      set("spark.cores.max", "50").
      set("spark.storage.memoryFraction", "0.6")
    ModuleClass.executor(Log2Parquet,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/metis/rawlog/metislog.access.log.$inputDate*"
          val outputPath = s"/log/metis/parquet/$inputDate/"

          val logRdd = sc.textFile(inputPath).repartition(repartitionNum).map(log => {
            val jsonedLog = LogUtils.logProcessMetis(log)
            if(jsonedLog != null){
              val (logType, jsonStr) = jsonedLog
              if (logType != "") {
                regex findFirstIn logType match {
                  case Some(_) => {
                    (logType, jsonStr)
                  }
                  case None => null
                }
              } else null
            } else null

          }).filter(_ != null).persist(StorageLevel.MEMORY_AND_DISK)

          val logTypeSet = logRdd.map(_._1).distinct().collect()

          logTypeSet.foreach(logType => {

            val jsonRdd = logRdd.filter(_._1 == logType).map(_._2)

            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath + logType)
            }
            sqlContext.read.json(jsonRdd).coalesce(outputPartionNum).write.parquet(outputPath + logType)


            logRdd.unpersist()
          })
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
