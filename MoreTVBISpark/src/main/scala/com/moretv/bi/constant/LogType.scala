package com.moretv.bi.constant

/**
 * Created by Will on 2015/9/19.
 * This object is a collect of log types constant.
 */
object LogType {

  val OPERATION = "operation"
  val PLAY = "play"
  val DETAIL = "detail"
  val COLLECT = "collect"
  val LIVE = "live"
  val INTERVIEW = "interview"
  val APPRECOMMEND = "apprecommend"
  val ENTER = "enter"
  val EXIT = "exit"
  val HOMEACCESS = "homeaccess"
  val MTV_ACCOUNT = "mtvaccount"
  val PAGEVIEW = "pageview"
  val POSITION_ACCESS = "positionaccess"
  val APPSUBJECT = "appsubject"
  val STAR_PAGE = "starpage"
  val RETRIEVAL = "retrieval"
  val SET = "set"
  val HOME_RECOMMEND = "homerecommend"
  val HOME_VIEW = "homeview"
  val DANMU_STATUS = "danmustatus"
  val DANMU_SWITCH = "danmuswitch"
  val QRCODE_SWITCH = "qrcodeswitch"


  //special log type which is apart from basic log type
  val PLAY_BAIDU_CLOUD = "play-baidu-cloud"
  val PLAYVIEW = "playview"
  val PLAYQOS = "playqos"
  val DETAIL_SUBJECT = "detail-subject"
  val OPERATION_E = "operation-e"
  val OPERATION_ACW = "operation-acw"
  val OPERATION_MM = "operation-mm"
  val OPERATION_ST = "operation-st"

  //corrupt log type
  val CORRUPT = "_corrupt"
//  val CORRUPT_OPERATION = "corrupt/operation"
//  val CORRUPT_PLAY = "corrupt/play"
//  val CORRUPT_DETAIL = "corrupt/detail"
//  val CORRUPT_COLLECT = "corrupt/collect"
//  val CORRUPT_LIVE = "corrupt/live"
}
