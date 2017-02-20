package com.moretv.bi.report.medusa.liveCastStat

/**
  * Created by witnes on 2/16/17.
  */
object DimForLive {

  /** Attributes **/

  val CATEGORYCODE = "liveMenuCode"

  val CATEGORYNAME = "liveMenuName"


  val PROGRAMENAME = "liveName"

  val CHANNELSID = "channelSid"


  val SOURCETYPE = "sourceType"

  val DAY = "date"

  val HOUR = "hour"

  val MINUTE = "minute"


  /** Metrics **/

  val VV = "vv"

  val UV = "uv"

  val DURATION = "duration"

  /** ***********************************************/


  val groupFields4D = Array(DAY, SOURCETYPE, CATEGORYCODE, CHANNELSID, PROGRAMENAME)

  val groupFields4M = Array(DAY, HOUR, MINUTE, SOURCETYPE, CATEGORYCODE, CHANNELSID, PROGRAMENAME)

  val groupFields4H = Array(DAY, HOUR, SOURCETYPE, CATEGORYCODE, CHANNELSID, PROGRAMENAME)

  val groupFields4CodeName = Array(CATEGORYCODE, CATEGORYNAME)

  val groupFields4DChannelPlay = Array(DAY, SOURCETYPE, CATEGORYNAME, CHANNELSID, PROGRAMENAME)


  val cube4DFields = groupFields4DChannelPlay ++ Array(UV, VV, DURATION)

  val cube4MFieldsU = groupFields4M ++ Array(UV, DURATION)

  val cube4MFieldsV = groupFields4M ++ Array(VV, DURATION)
}
