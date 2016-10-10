package com.moretv.bi.temp

import java.util.Calendar

import cn.whaley.turbo.forest.core.ProcessLog
import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel
import org.json.JSONObject

/**
 * Created by Will on 2015/8/20.
 */
object Log2Parquet extends BaseClass{

  private val outputPartitionNum = 40
  val regex = ("\\{\"remote_addr\":\"([0-9\\.]+)\",\"time\":\"([^ ]+).+?\"requestBody\":\"([^\"]+)" +
    "\",\"status\".+?\"ip_forwarded\":\"([0-9\\.]+)").r

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
          val inputPath = s"/log/test/meudsa/rawlog/*"
          val outputPath = s"/log/meudsa/parquet/$inputDate/"
          val logTypes = p.logTypes

          val logRdd = sc.textFile(inputPath)

          val flattenRdd = logRdd.map(line => {
            regex findFirstMatchIn line match {
              case Some(m) => {
                val remoteIp = m.group(1)
                val time = m.group(2)
                val body = m.group(3)
                val forwardIp = m.group(4)
                val ip = if(forwardIp != "") forwardIp else remoteIp
                val jsonStr = Hex2StringUtil.hex2String(body)
                val Array(date,datetime) = DateFormatUtils.toCNDateArray(time)

                val json = new JSONObject(jsonStr)
                ProcessLog.logFlattening(json).map(js => {
                  js.put("realIP",ip)
                  js.put("date",date)
                  js.put("day",date)
                  js.put("datetime",datetime)
                  val logType = js.optString("logType")
                  val id = if(logType == "event") {
                    js.optString("eventId")
                  } else if (logType == "start_end"){
                    js.optString("actionId")
                  }else ""
                  (logType,id,js.toString())
                })
              }
              case None => null
            }
          }).filter(_ != null).flatMap(x => x).persist(StorageLevel.MEMORY_AND_DISK_SER)
          val logTypeEvents = flattenRdd.map(e => (e._1,e._2)).distinct().collect()
          logTypeEvents.foreach(x => {

          })
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
