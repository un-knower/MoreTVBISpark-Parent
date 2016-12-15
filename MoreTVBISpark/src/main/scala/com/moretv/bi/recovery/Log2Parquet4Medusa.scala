package com.moretv.bi.recovery

import java.util.Calendar

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by Will on 2015/8/20.
 */
object Log2Parquet4Medusa extends BaseClass{

  private val outputPartitionNum = 40

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.cores.max", "200").
      set("spark.executor.cores", "5").
      set("spark.storage.memoryFraction", "0.1")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val regexWord = "^\\w+$".r
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/medusa/rawlog/$inputDate/*"
          val outputPath = s"/log/medusa/parquet/$inputDate/"
          val logType = "play"

          val df = sqlContext.read.parquet(outputPath + logType)
          val columns = df.columns
          val arr = columns.map(column => {
            regexWord findFirstIn column match {
              case Some(m) => column
              case None => null
            }
          }).filter(_ != null)
          val columnStr = arr.mkString(",")
          df.select(arr.head,arr.tail:_*)
          s"select $columnStr from log_data"
          val needRerun = columns.exists(column => {
            regexWord findFirstIn column match {
              case Some(m) => false
              case None => true
            }
          })
          if(needRerun){
            val logRdd = sc.textFile(inputPath)

            val jsonRdd = logRdd.map(log => {
              if(log.contains(s"logType=$logType") && !UserBlackListUtil.isBlack(log)){
                val jsonLog = LogUtils.logProcessMedusa(log)
                if(jsonLog != null){
                  val (logType, userId, jsonStr) = jsonLog
                  if (logType == logType) {
                    if(!DevMacUtils.userIdFilter(userId)) jsonStr else null
                  }else null
                } else null
              } else null
            }).filter(_ != null)
            HdfsUtil.deleteHDFSFile(outputPath + logType)
            sqlContext.read.json(jsonRdd).coalesce(outputPartitionNum).write.parquet(outputPath + logType)
          }

          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

}
