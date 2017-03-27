package com.moretv.bi.report.medusa.liveCastStat

/**
  * Created by witnes on 2/16/17.
  */
object DimForLive {

  /** Attributes **/


  val PROGRAMENAME = "liveName"

  val CHANNELSID = "channelSid"


  val SOURCETYPE = "sourceType"

  val DAY = "date"

  val HOUR = "hour"

  val MINUTE = "minute"

  val HOUR_MINUTE = "hourMinute"

  val PERIOD = "period"

  val LIVECATEGORYCODE = "liveMenuCode"

  val LIVECATEGORYNAME = "liveMenuName"


  /** Metrics **/

  val VV = "vv"

  val UV = "uv"

  val DURATION = "duration"

  /** ***********************************************/

  /** time sourcetype category channel uv vv duration**/

  val groupFields4D = Array(DAY, SOURCETYPE, LIVECATEGORYCODE, CHANNELSID, PROGRAMENAME)

  val groupFields4H = Array(DAY, HOUR, SOURCETYPE, LIVECATEGORYCODE, CHANNELSID, PROGRAMENAME)

  val groupFields4M = Array(DAY, HOUR, MINUTE, SOURCETYPE, LIVECATEGORYCODE, CHANNELSID, PROGRAMENAME)


  val groupFields4Category = Array(LIVECATEGORYCODE, LIVECATEGORYNAME)

  val groupFields4DAttr = Array(DAY, SOURCETYPE, LIVECATEGORYCODE,LIVECATEGORYNAME, CHANNELSID, PROGRAMENAME)


  val cube4DFieldsUVD = groupFields4DAttr ++ Array(UV, VV, DURATION)

  val cube4MFieldsU = groupFields4M ++ Array(LIVECATEGORYNAME,UV)

  val cube4MFieldsV = groupFields4M ++ Array(LIVECATEGORYNAME,VV)


  /**  ES Source Info **/

  val ES_INDEX = "medusa"

  val ES_TYPE_M = "channelMinutePlay"

  val ES_TYPE_10M = "channel10MinutePlay"

  val ES_TYPE_D = "channelDayPlay"
}
