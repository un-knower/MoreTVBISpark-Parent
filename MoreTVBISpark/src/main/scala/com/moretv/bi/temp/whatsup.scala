package com.moretv.bi.temp

import java.util.Calendar

import com.moretv.bi.template.StatHandler.{PlayLiveExecutor, SqlPattern}
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 11/2/16.
  */
object whatsup extends BaseClass {

  private val loadDate = "2016{12*}"

  private val loadPlay23 = s"/log/medusaAndMoretvMerger/$loadDate/playview"


  def main(args: Array[String]) {
    ModuleClass.executor(whatsup, args)
  }


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        p.logType match {

          case "play" => playHandler(p.srcPath, p.outputFile)

          case "live" => liveHandler(p.srcPath, p.outputFile)

        }
      }
      case None => {

      }
    }
  }

  def liveHandler(srcPath: String, outputFile: String) = {
    val schema = "channelSid,channelName,startTime,endTime"
    PlayLiveExecutor.loadSourceData("live", loadDate)
    PlayLiveExecutor.loadFilterCondition(schema, srcPath)
    PlayLiveExecutor.withoutVersionStat(outputFile)
  }

  def playHandler(srcPath: String, outputFile: String) = {

    val schema = "date,videoSid,videoName,startTime,endTime"

  }


}
