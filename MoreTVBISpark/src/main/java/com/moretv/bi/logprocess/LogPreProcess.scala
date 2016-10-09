package com.moretv.bi.logprocess

import java.net.URLDecoder

import com.moretv.bi.constant.LogFields._
import com.moretv.bi.constant.LogType._
import com.moretv.bi.logprocess.LogRegexs._
import com.moretv.bi.util.{RangeUtil, LogUtils, DateFormatUtils}
import org.apache.http.conn.ssl.AllowAllHostnameVerifier

/**
 * Created by Will on 2015/9/19.
 * This program transformed the string log to an array of filed.
 */
object LogPreProcess {

  /**
   * process the log of each log type
   * @param logType the log type of the log
   * @param log the raw log
   * @return
   */
  def matchLog(logType:String,log:String) = logType match {
    case OPERATION => operationMatch(log)
    case PLAY => playMatch(log)
    case COLLECT => collectMatch(log)
    case LIVE => liveMatch(log)
    case DETAIL => detailMatch(log)
    case INTERVIEW => interviewMatch(log)
    case ENTER => enterMatch(log)
    case EXIT => exitMatch(log)
    case APPRECOMMEND => apprecommendMatch(log)
    case HOMEACCESS => homeaccessMatch(log)
    case MTV_ACCOUNT => mtvaccountMatch(log)
    case PAGEVIEW => pageviewMatch(log)
    case POSITION_ACCESS => positionaccessMatch(log)
    case APPSUBJECT => appsubjectMatch(log)
    case STAR_PAGE => starpageMatch(log)
    case RETRIEVAL => retrievalMatch(log)
    case SET => setMatch(log)
    case HOME_RECOMMEND => homerecommendMatch(log)
    case HOME_VIEW => homeviewMatch(log)
    case DANMU_STATUS  => danmustatusMatch(log)
    case DANMU_SWITCH  => danmuswitchMatch(log)
//    case QRCODE_SWITCH => qrcodeswitchMatch(log)
    case _ => toCaseClass(logType = CORRUPT,rawLog = log)
  }

  //To assembly the case class
  def toCaseClass(date:String = "",
                  datetime:String = "",
                  ip:String = "",
                  logType:String = "",
                  logVersion:String = "",
                  event:String = "",
                  apkSeries:String = "",
                  apkVersion:String = "",
                  userId:String = "",
                  accountId:Int = 0,
                  groupId:Int = 0,
                  bufferTimes:Int = 0,
                  duration:Int = 0,
                  liveType:String = "",
                  channelSid:String = "",
                  path:String = "",
                  subjectCode:String = "",
                  source:String = "",
                  contentType:String = "",
                  videoSid:String = "",
                  episodeSid:String = "",
                  collectType:String = "",
                  collectContent:String = "",
                  action:String = "",
                  promotionChannel:String = "",
                  weatherCode:String = "",
                  productModel:String = "",
                  uploadTime:String = "",
                  page:String = "",
                  appSid:String = "",
                  accessSource:String = "",
                  accessArea:String = "",
                  accessLocation:String = "",
                  star:String = "",
                  retrievalSort:String = "",
                  retrievalArea:String = "",
                  retrievalYear:String = "",
                  retrievalTag:String = "",
                  wallpaper:String = "",
                  homeType:String = "",
                  homeContent:String = "",
                  homeLocation:String = "",
                  wechatId:String = "",
                  title:String = "",
                  linkType:String = "",
                  programSid:String = "",
                  programType:String = "",
                  rawLog:String = "")
                = new MoreTVLogData(
                  date,
                  datetime,
                  ip,
                  logType,
                  logVersion,
                  event,
                  apkSeries,
                  apkVersion,
                  userId,
                  accountId,
                  groupId,
                  bufferTimes,
                  duration,
                  liveType,
                  channelSid,
                  path,
                  subjectCode,
                  source,
                  contentType,
                  videoSid,
                  episodeSid,
                  collectType,
                  collectContent,
                  action,
                  promotionChannel,
                  weatherCode,
                  productModel,
                  uploadTime,
                  page,
                  appSid,
                  accessSource,
                  accessArea,
                  accessLocation,
                  star,
                  retrievalSort,
                  retrievalArea,
                  retrievalYear,
                  retrievalTag,
                  wallpaper,
                  homeType,
                  homeContent,
                  homeLocation,
                  wechatId,
                  title,
                  linkType,
                  programSid,
                  programType,
                  rawLog)

  /**
   * process operation log
   * @param log raw string log
   * @return
   */
  def operationMatch(log:String)={
    regexOperationEvaluate findFirstMatchIn log match {
      case Some(m) => {
        val dateStr = m.group(1)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(2)
        val seriesVersion = m.group(3)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(4)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(5)
        val weatherCode = m.group(6)
        val productModel = m.group(7)
        val uploadTime = m.group(8)
        val videoSid = m.group(9)
        val contentType = m.group(10)
        val action = m.group(11)

        toCaseClass(date = date,datetime = datetime,logType = OPERATION_E,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,videoSid = videoSid,contentType = contentType,action = action)
      }
      case None => regexOperationACW findFirstMatchIn log match {
        case Some(m) => {
          val dateStr = m.group(1)
          val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
          val event = m.group(2)
          val seriesVersion = m.group(3)
          val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
          val idsStr = m.group(4)
          val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
          val promotionChannel = m.group(5)
          val weatherCode = m.group(6)
          val productModel = m.group(7)
          val uploadTime = m.group(8)
          val contentType = m.group(9)
          val videoSid = m.group(10)

          toCaseClass(date = date,datetime = datetime,logType = OPERATION_ACW,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
            apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
            promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
            uploadTime = uploadTime,videoSid = videoSid,contentType = contentType)
        }
        case None => regexOperationMM findFirstMatchIn log match {
          case Some(m) => {
            val dateStr = m.group(1)
            val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
            val event = m.group(2)
            val seriesVersion = m.group(3)
            val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
            val idsStr = m.group(4)
            val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
            val promotionChannel = m.group(5)
            val weatherCode = m.group(6)
            val productModel = m.group(7)
            val uploadTime = m.group(8)

            toCaseClass(date = date,datetime = datetime,logType = OPERATION_MM,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
              apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
              promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
              uploadTime = uploadTime)
          }
          case None => regexOperationShowlivelist findFirstMatchIn log match {
            case Some(m) => {
              val dateStr = m.group(1)
              val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
              val event = m.group(2)
              val seriesVersion = m.group(3)
              val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
              val idsStr = m.group(4)
              val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
              val promotionChannel = m.group(5)
              val weatherCode = m.group(6)
              val productModel = m.group(7)
              val uploadTime = m.group(8)
              val channelSid = m.group(9)

              toCaseClass(date = date,datetime = datetime,logType = OPERATION_ST,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
                apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
                promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
                uploadTime = uploadTime,channelSid = channelSid)
            }
            case None => regexOperationTimeshifting findFirstMatchIn log match {
              case Some(m) => {
                val dateStr = m.group(1)
                val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
                val event = m.group(2)
                val seriesVersion = m.group(3)
                val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
                val idsStr = m.group(4)
                val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
                val promotionChannel = m.group(5)
                val weatherCode = m.group(6)
                val productModel = m.group(7)
                val uploadTime = m.group(8)
                val channelSid = m.group(9)

                toCaseClass(date = date,datetime = datetime,logType = OPERATION_ST,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
                  apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
                  promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
                  uploadTime = uploadTime,channelSid = channelSid)
              }
              case None => toCaseClass(logType = CORRUPT,rawLog = log)
            }
          }
        }
      }
    }

  }

  /**
   * process collect log
   * @param log raw string log
   * @return
   */
  def collectMatch(log:String)={
    regexCollect findFirstMatchIn log match {
      case Some(m) => {
        val dateStr = m.group(1)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(2)
        val seriesVersion = m.group(3)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(4)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val collectType = m.group(5)
        val collectContent = m.group(6)
        val promotionChannel = m.group(7)
        val weatherCode = m.group(8)
        val productModel = m.group(9)
        val uploadTime = m.group(10)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = COLLECT,collectType = collectType,collectContent = collectContent)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  /**
   * process detail log
   * @param log raw string log
   * @return
   */
  def detailMatch(log:String)={
    regexDetail findFirstMatchIn log match {
      case Some(m) => {
        val dateStr = m.group(1)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(2)
        val seriesVersion = m.group(3)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(4)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val path = m.group(5)
        val contentType = m.group(6)
        val videoSid = m.group(7)
        val promotionChannel = m.group(8)
        val weatherCode = m.group(9)
        val productModel = m.group(10)
        val uploadTime = m.group(11)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = DETAIL,path = path,contentType = contentType,videoSid = videoSid)
      }
      case None => regexDetailSubject findFirstMatchIn log match {
        case Some(m) => {
          val dateStr = m.group(1)
          val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
          val event = m.group(2)
          val seriesVersion = m.group(3)
          val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
          val idsStr = m.group(4)
          val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
          val path = m.group(5)
          val subjectCode = m.group(6)
          val promotionChannel = m.group(7)
          val weatherCode = m.group(8)
          val productModel = m.group(9)
          val uploadTime = m.group(10)

          toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
            apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
            promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
            uploadTime = uploadTime,logType = DETAIL_SUBJECT,path = path,subjectCode = subjectCode)
        }
        case None => toCaseClass(logType = CORRUPT,rawLog = log)
      }
    }
  }

  /**
   * process play log
   * @param log raw string log
   * @return
   */
  def playMatch(log:String)= {
    regexPlayview findFirstMatchIn log match {
      case Some(m) => {
        val dateStr = m.group(1)
        val Array(date, datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(2)
        val seriesVersion = m.group(3)
        val Array(series, version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(4)
        val (userId, accountId, groupId) = LogUtils.getIds(idsStr)
        val path = m.group(5)
        val contentType = m.group(6)
        val videoSid = m.group(7)
        val episodeSid = m.group(8)
        val duration = m.group(9)
        val promotionChannel = m.group(10)
        val weatherCode = m.group(11)
        val productModel = m.group(12)
        val uploadTime = m.group(13)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,event = event,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = PLAYVIEW,path = path,contentType = contentType,videoSid = videoSid,
          episodeSid = episodeSid,duration = duration.toInt)
      }
      case None =>
        regexPlayqos findFirstMatchIn log match {
          case Some(m) => {
            val dateStr = m.group(1)
            val Array(date, datetime) = DateFormatUtils.toCNDateArray(dateStr)
            val event = m.group(2)
            val seriesVersion = m.group(3)
            val Array(series, version) = LogUtils.splitSeriesVersion(seriesVersion)
            val idsStr = m.group(4)
            val (userId, accountId, groupId) = LogUtils.getIds(idsStr)
            val path = m.group(5)
            val source = m.group(6)
            val contentType = m.group(7)
            val videoSid = m.group(8)
            val episodeSid = m.group(9)
            val bufferTimes = m.group(10)
            val duration = m.group(11)
            val promotionChannel = m.group(12)
            val weatherCode = m.group(13)
            val productModel = m.group(14)
            val uploadTime = m.group(15)

            toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
              apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
              promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
              uploadTime = uploadTime,logType = PLAYQOS,event = event,path = path,source = source,
              contentType = contentType,videoSid = videoSid,
              episodeSid = episodeSid,bufferTimes = bufferTimes.toInt,duration = duration.toInt)
          }
          case None => regexPlayBaiduCloud findFirstMatchIn log match {
            case Some(m) => {
              val dateStr = m.group(1)
              val Array(date, datetime) = DateFormatUtils.toCNDateArray(dateStr)
              val event = m.group(2)
              val seriesVersion = m.group(3)
              val Array(series, version) = LogUtils.splitSeriesVersion(seriesVersion)
              val idsStr = m.group(4)
              val (userId, accountId, groupId) = LogUtils.getIds(idsStr)
              val path = m.group(5)
              val duration = m.group(7)
              val promotionChannel = m.group(8)
              val weatherCode = m.group(9)
              val productModel = m.group(10)
              val uploadTime = m.group(11)

              toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
                apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
                promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
                uploadTime = uploadTime,logType = PLAY_BAIDU_CLOUD,event = event,path = path,duration = duration.toInt)
            }
            case None => toCaseClass(logType = CORRUPT,rawLog = log)
          }

        }
    }
  }

  /**
   * process interview log
   * @param log raw string log
   * @return
   */
  def interviewMatch(log: String) = {
    regexInterviewEnter findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val path = m.group(i.next)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = INTERVIEW,event = "enter",path = path)
      }
      case None => regexInterviewExit findFirstMatchIn log match {
        case Some(m) => {
          val i = RangeUtil(1,1)
          val dateStr = m.group(i.next)
          val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
          val seriesVersion = m.group(i.next)
          val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
          val idsStr = m.group(i.next)
          val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
          val path = m.group(i.next)
          val duration = m.group(i.next)
          val promotionChannel = m.group(i.next)
          val weatherCode = m.group(i.next)
          val productModel = m.group(i.next)
          val uploadTime = m.group(i.next)

          toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
            apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
            promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
            uploadTime = uploadTime,logType = INTERVIEW,event = "exit",path = path,duration = duration.toInt)
        }
        case None => toCaseClass(logType = CORRUPT,rawLog = log)
      }

    }

  }

  /**
   * process live log
   * @param log raw string log
   * @return
   */
  def liveMatch(log: String) = {
    regexLive findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val path = m.group(i.next)
        val liveTypeCode = m.group(i.next)
        val liveType = if(liveTypeCode == "1") "live" else if(liveTypeCode == "2") "past" else "other"
        val channelSid = m.group(i.next)
        val duration = m.group(i.next)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = LIVE,path = path,liveType = liveType,channelSid = channelSid,duration = duration.toInt)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)

    }

  }

  /**
   * process apprecommend log
   * @param log raw string log
   * @return
   */
  def apprecommendMatch(log: String) = {
    regexApprecommend findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val asStr = m.group(i.next)
        val (appSid,subjectCode) = LogUtils.getAppIdAndSubjectCode(asStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = APPRECOMMEND,event = event,appSid = appSid,subjectCode = subjectCode)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)

    }

  }

  def enterMatch(log: String) = {
    regexEnter findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val accessSource = m.group(i.next)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = ENTER,accessSource = accessSource)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)

    }

  }

  def exitMatch(log: String) = {
    regexExit findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val accessSource = m.group(i.next)
        val duration = m.group(i.next)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = EXIT,accessSource = accessSource,duration = duration.toInt)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)

    }

  }

  def homeaccessMatch(log: String) = {
    regexHomeaccess findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val accessArea = m.group(i.next)
        val accessLocation = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = HOMEACCESS,event = event,accessArea = accessArea,accessLocation = accessLocation)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def mtvaccountMatch(log: String) = {
    regexMtvaccountLogin findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val path = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = MTV_ACCOUNT,event = event,path = path)
      }
      case None => regexMtvaccount findFirstMatchIn log match {
        case Some(m) => {
          val i = RangeUtil(1,1)
          val dateStr = m.group(i.next)
          val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
          val event = m.group(i.next)
          val seriesVersion = m.group(i.next)
          val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
          val idsStr = m.group(i.next)
          val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
          val promotionChannel = m.group(i.next)
          val weatherCode = m.group(i.next)
          val productModel = m.group(i.next)
          val uploadTime = m.group(i.next)

          toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
            apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
            promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
            uploadTime = uploadTime,logType = MTV_ACCOUNT,event = event)
        }
        case None => toCaseClass(logType = CORRUPT,rawLog = log)
      }
    }
  }

  def pageviewMatch(log: String) = {
    regexPageview findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val ppStr = m.group(i.next)
        val (page,path) = LogUtils.getPagePath(ppStr)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = PAGEVIEW,event = event,page = page,path = path)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def positionaccessMatch(log: String) = {
    regexPositionaccess findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val accessArea = m.group(i.next)
        val accessLocation = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = POSITION_ACCESS,event = event,accessArea = accessArea,accessLocation = accessLocation)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def appsubjectMatch(log: String) = {
    regexAppsubject findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val subjectCode = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = APPSUBJECT,event = event,subjectCode = subjectCode)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def starpageMatch(log: String) = {
    regexStarpage findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val star = URLDecoder.decode(m.group(i.next),"UTF-8")
        val path = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = STAR_PAGE,event = event,star = star,path = path)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def retrievalMatch(log: String) = {
    regexRetrieval findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val path = m.group(i.next)
        val retrievalSort = m.group(i.next)
        val retrievalArea = m.group(i.next)
        val retrievalYear = m.group(i.next)
        val retrievalTag = m.group(i.next)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = RETRIEVAL,path = path,retrievalSort = retrievalSort,
          retrievalArea = retrievalArea,retrievalYear = retrievalYear,retrievalTag = retrievalTag)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def setMatch(log: String) = {
    regexSetWallpaper findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val wallpaper = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = SET,event = event,wallpaper = wallpaper)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def homerecommendMatch(log: String) = {
    regexHomeRecommend findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val homeType = m.group(i.next)
        val homeContent = m.group(i.next)
        val homeLocation = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = HOME_RECOMMEND,event = event,homeType = homeType,homeContent = homeContent,
          homeLocation = homeLocation)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def homeviewMatch(log: String) = {
    regexHomeviewEnter findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = HOME_VIEW,event = "enter")
      }
      case None => regexHomeviewExit findFirstMatchIn log match {
        case Some(m) => {
          val i = RangeUtil(1,1)
          val dateStr = m.group(i.next)
          val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
          val seriesVersion = m.group(i.next)
          val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
          val idsStr = m.group(i.next)
          val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
          val promotionChannel = m.group(i.next)
          val weatherCode = m.group(i.next)
          val productModel = m.group(i.next)
          val uploadTime = m.group(i.next)
          val duration = m.group(i.next)

          toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
            apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
            promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
            uploadTime = uploadTime,logType = HOME_VIEW,event = "exit",duration = duration.toInt)
        }
        case None => toCaseClass(logType = CORRUPT,rawLog = log)
      }

    }

  }

  def danmustatusMatch(log: String) = {
    regexDanmustatus findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = DANMU_STATUS,event = event)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

  def danmuswitchMatch(log: String) = {
    regexDanmuswitch findFirstMatchIn log match {
      case Some(m) => {
        val i = RangeUtil(1,1)
        val dateStr = m.group(i.next)
        val Array(date,datetime) = DateFormatUtils.toCNDateArray(dateStr)
        val event = m.group(i.next)
        val seriesVersion = m.group(i.next)
        val Array(series,version) = LogUtils.splitSeriesVersion(seriesVersion)
        val idsStr = m.group(i.next)
        val (userId,accountId,groupId) = LogUtils.getIds(idsStr)
        val promotionChannel = m.group(i.next)
        val weatherCode = m.group(i.next)
        val productModel = m.group(i.next)
        val uploadTime = m.group(i.next)
        val programSid = m.group(i.next)
        val programType = m.group(i.next)

        toCaseClass(date = date,datetime = datetime,logVersion = LOG_VERSION_DEFAULT,apkSeries = series,
          apkVersion = version,userId = userId,accountId = accountId.toInt,groupId = groupId.toInt,
          promotionChannel = promotionChannel,weatherCode = weatherCode,productModel = productModel,
          uploadTime = uploadTime,logType = DANMU_SWITCH,event = event,programSid = programSid, programType = programType)
      }
      case None => toCaseClass(logType = CORRUPT,rawLog = log)
    }
  }

}
