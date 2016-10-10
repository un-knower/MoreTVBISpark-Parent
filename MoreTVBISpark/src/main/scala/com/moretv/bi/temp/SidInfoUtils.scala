package com.moretv.bi.temp

import java.io.{File, PrintWriter}

import org.json.JSONObject

import scala.io.Source


/**
  * Created by Will on 2016/8/28.
  */
object SidInfoUtils {

  val regex = "sid = (\\w{12})".r

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val in = new File(inputPath)
    val lines = Source.fromFile(in).getLines()
    val completedLines = lines.map(line => {
      regex findFirstMatchIn line match {
        case Some(m) => {
          val sid = m.group(1)
          val info = ProgramRedisUtil.getInfoBySid(sid)
          if(info != null){
            val json = new JSONObject(info)
            var title = json.optString(ProgramRedisUtil.DISPLAY_NAME)
            if (title != "") {
              title = title.replace("'", "")
              title = title.replace("\t", " ")
              title = title.replace("\r", "-")
              title = title.replace("\n", "-")
              title = title.replace("\r\n", "-")
            } else title = "sid not found"
            val contentType = json.optString(ProgramRedisUtil.CONTENT_TYPE)
            Array(line,title,contentType)
          }else Array(line,"sid not found")
        }
        case None => Array(line,"sid not found")
      }
    })
    val out = new File(outputPath)
    val writer = new PrintWriter(out)
    completedLines.foreach(arr => {
      val str = arr.mkString(",")
      writer.println(str)
    })
    writer.close()
  }
}
