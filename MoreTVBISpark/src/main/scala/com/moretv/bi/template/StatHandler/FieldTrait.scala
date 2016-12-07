package com.moretv.bi.template.StatHandler

/**
  * Created by witnes on 11/3/16.
  */
trait FieldTrait {

  val TIMETYPE = Map(

    "play" -> DATE,

    "live" -> DATETIME
  )

  val VIDEOSID = "tbl2.videoSid"

  val VIDEONAME = "tbl2.videoName"

  val VERSION = "tbl1.version"

  val STARTTIME = "tbl2.startTime"

  val ENDTIME = "tbl2.endTime"

  val STARTTIME_ = "startTime"

  val ENDTIME_ = "startTime"

  val VIDEOSID_ = "videoSid"

  val VIDEONAME_ = "videoName"

  val VERSION_ = "version"


  //// time area

  val DATE = "tbl1.date"

  val DATETIME = "tbl1.datetime"

  val MONTH = "substr(tbl1.date,6,2)"

  //// agg

  val PV = "count(tbl1.userId) as pv"

  val UV = "count(distinct tbl1.userId) as uv"




}
