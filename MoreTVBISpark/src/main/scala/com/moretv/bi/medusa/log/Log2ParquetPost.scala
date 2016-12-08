package com.moretv.bi.medusa.log

import java.util.Calendar

import cn.whaley.turbo.forest.core.ProcessLog
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel
import org.json.JSONObject

import scala.collection.JavaConversions._

/**
 * Created by Will on 2015/8/20.
 */
object Log2ParquetPost extends BaseClass{

  private val rePartitionNum = 40
  val regex = ("\\{\"remote_addr\":\"([0-9\\.]+)\",\"time\":\"([^ ]+).+?\"requestBody\":\"([^\"]+)" +
    "\",\"status\".+?\"ip_forwarded\":\"([0-9\\.]+)").r
  val regexWord = "^\\w+$".r
  val regexEventType = "^medusa-\\w+-([\\w\\-]+)$".r

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "10g").
      set("spark.cores.max", "200").
      set("spark.executor.cores", "5").
      set("spark.storage.memoryFraction", "0.5")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/medusa/rawlog-post/$inputDate/*"
          val outputPath = s"/log/medusa/parquet/$inputDate/"

          val logRdd = sc.textFile(inputPath).filter(_.contains(""""method":"POST""""))
          val flattenRdd = logRdd.map(line => {
            val json = new JSONObject(line)
            try {
                ProcessLog.logFlattening(json).map(js => {
                  js.keys().foreach(key => {
                    regexWord findFirstIn key match {
                      case Some(k) =>
                      case None => js.remove(key)
                    }
                  })
                  val logType = js.optString("logType")
                  val id = if (logType == "event") {
                    js.optString("eventId")
                  } else if (logType == "start_end") {
                    js.optString("actionId")
                  } else ""
                  if(id != ""){
                    regexEventType findFirstMatchIn id match {
                      case Some(m) => {
                        val eventType = m.group(1)
                        if(eventType == "tabview") {
                          ("search-tabview",js.toString)
                        }else if(id.endsWith("clickResult")){
                          if(id == "medusa-mv_station-clickResult"){
                            (id,js.toString)
                          }else if(id == "medusa-search-clickResult"){
                            ("clickSearchResult",js.toString)
                          }else {
                            if(js.has("clickWidget")) ("searchEntrance",js.toString) else (id,js.toString)
                          }
                        }else (eventType,js.toString)
                      }
                      case None => null
                    }
                  }else null
                }).filter(_ != null)
            } catch {
              case e:Exception => null
            }
          }).filter(_ != null).flatMap(x => x).persist(StorageLevel.MEMORY_AND_DISK_SER)

          val eventIds = flattenRdd.map(_._1).distinct().collect()
          eventIds.foreach(eventId => {
            if(eventId != ""){
              val jsonRdd = flattenRdd.map(e => {
                val (id,jsonStr) = e
                if(id == eventId  && !UserBlackListUtil.isBlack(jsonStr)) jsonStr else null
              }).filter(_ != null)
              if (p.deleteOld) {
                HdfsUtil.deleteHDFSFile(outputPath + eventId)
              }
              sqlContext.read.json(jsonRdd).write.parquet(outputPath + eventId)
            }
          })

          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

}
