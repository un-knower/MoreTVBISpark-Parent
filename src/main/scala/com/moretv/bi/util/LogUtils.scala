package com.moretv.bi.util

import java.net.URLDecoder

import com.moretv.bi.constant.LogFields
import com.moretv.bi.constant.LogKey._
import org.json.JSONObject

/**
 * Created by Will on 2015/7/16.
 * The utility of log str process.
 */
@deprecated
object LogUtils {

  val regexIpA = "\"(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})".r
  val regexIpB = "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})".r
  val invalidChars = List(" ",",",";","{","}","(",")","\\n","\\t","=","/","\\","..","`",
    "!","@","#","$","%","^","&","*","'",":","[","]","?","<",">")
  val regexWord = "^\\w+$".r

  /**
    * transform log str to json object
    *
    * @param log log str
   * @return json object
   */
  def log2json(log:String) = {
    val startIdx = log.indexOf('?')
    val endIdx = log.indexOf(' ', startIdx)

    if (startIdx > 0 && endIdx > startIdx) {
      val dateIdx = log.indexOf('[')
      val dateTimeStr = log.substring(dateIdx + 1, dateIdx + 21)
      val dateTimeStrCN = DateFormatUtils.enFormat2CNFormat(dateTimeStr)
      if(dateTimeStrCN != null){
        val dateCN = dateTimeStrCN.substring(0, 10)
        val json = new JSONObject()
        regexIpA findFirstMatchIn log match{
          case Some(i) => json.put("ip",i.group(1))
          case None => regexIpB findFirstMatchIn log match {
            case Some(p) => json.put("ip",p.group(1))
            case None => json.put("ip","")
          }
        }
        json.put("date",dateCN)
        json.put("day",dateCN)
        json.put("datetime",dateTimeStrCN)
        val content = log.substring(startIdx + 1, endIdx)
        val entries = content.split("\\&")
        for (entry <- entries) {
          val kv = entry.split("=")
          if (kv.length == 1) {
            val key = keyProcess(kv(0))

            if(key == "duration"|| key == "accountId"){
              json.put(key, 0)
            }else json.put(key, "")
          }
          else if (kv.length == 2) {
            try {
              val decoded = URLDecoder.decode(kv(1), "utf-8")
              val key = keyProcess(kv(0))
              if(key == "duration" || key == "accountId"){
                try {
                  json.put(key, decoded.toLong)
                } catch {
                  case e:Exception => json.put(key, 0)
                }
              }else json.put(key, decoded)
            } catch {
              case e:Exception => json.put(kv(0), "")
            }
          }
        }
        json
      }else null
    }else null
  }


  /**
    * transform log str to json object
    *
    * @param log log str
    * @return json object
    */
  def loginlog2json(log:String) = {
    val startIdx = log.indexOf('?')
    val endIdx = log.indexOf(' ', startIdx)

    if (startIdx > 0 && endIdx > startIdx) {
      val dateIdx = log.indexOf('[')
      val dateTimeStr = log.substring(dateIdx + 1, dateIdx + 21)
      val dateTimeStrCN = DateFormatUtils.enFormat2CNFormat(dateTimeStr)
      if(dateTimeStrCN != null){
        val dateCN = dateTimeStrCN.substring(0, 10)
        val json = new JSONObject()
        regexIpA findFirstMatchIn log match{
          case Some(i) => json.put("ip",i.group(1))
          case None => regexIpB findFirstMatchIn log match {
            case Some(p) => json.put("ip",p.group(1))
            case None => json.put("ip","")
          }
        }
        json.put("date",dateCN)
        json.put("day",dateCN)
        json.put("datetime",dateTimeStrCN)
        val content = log.substring(startIdx + 1, endIdx)
        val entries = content.split("\\&")
        for (entry <- entries) {
          val kv = entry.split("=")
          if (kv.length == 1) {
            json.put(keyProcess(kv(0)), "")
          }
          else if (kv.length == 2){
            try {
              val decoded = URLDecoder.decode(kv(1), "utf-8")
              json.put(keyProcess(kv(0)), decoded)
            } catch {
              case e:Exception => json.put(keyProcess(kv(0)), "")
            }
          }
        }
        json
      }else null
    }else null
  }

  /**
   * transform log str to json object
    *
    * @param log log str
   * @return json object
   */
  def log2jsonLite(log:String) = {
    val startIdx = log.indexOf('?')
    val endIdx = log.indexOf(' ', startIdx)

    if (startIdx > 0 && endIdx > startIdx) {
      val content = log.substring(startIdx + 1, endIdx)
      val entries = content.split("\\&")
      val json = new JSONObject()
      for (entry <- entries) {
        val kv = entry.split("=")
        if (kv.length < 2) {
          json.put(kv(0), "")
        }
        else {
          json.put(kv(0), kv(1))
        }
      }
      json
    }else null
  }

  /**
   * split seriesVersion
    *
    * @param seriesVersion series_version
   * @return (series,version)
   */
  def splitSeriesVersion(seriesVersion: String) = {
    val idx = seriesVersion.lastIndexOf('_')
    if(idx < 0) Array("","") else
    Array(seriesVersion.substring(0,idx),seriesVersion.substring(idx+1))
  }

  /**
   * split userId,accountId,groupId
    *
    * @param idsStr userId-accountId-groupId str
   * @return (userId,accountId,groupId)
   */
  def getIds(idsStr: String) = {
    val ids = idsStr.split("-")
    if(ids.length == 3){
      val userId = ids(0)
      val accountId = if(ids(1).length > 0) ids(1) else LogFields.ACCOUNT_ID_DEFAULT
      val groupId = if(ids(2).length > 0) ids(2) else LogFields.GROUP_ID_DEFAULT
      (userId,accountId,groupId)
    }else if(ids.length == 2){
      val userId = ids(0)
      val accountId = if(ids(1).length > 0) ids(1) else LogFields.ACCOUNT_ID_DEFAULT
      (userId,accountId,LogFields.GROUP_ID_DEFAULT)
    }else (ids(0),LogFields.ACCOUNT_ID_DEFAULT,LogFields.GROUP_ID_DEFAULT)
  }

  /**
   * split appId,subjectCode
    *
    * @param idStr appId-subjectCode
   * @return (appId,subjectCode)
   */
  def getAppIdAndSubjectCode(idStr:String) = {
    val ids = idStr.split("-")
    if(ids.length == 2){
      (ids(0),ids(1))
    }else (ids(0),"")
  }

  /**
   * get page & path from pageview raw log
    *
    * @param ppStr part of the raw log
   * @return (page,path)
   */
  def getPagePath(ppStr:String) = {
    val idxPage = ppStr.indexOf("%26page%3D")
    val idxPath = ppStr.indexOf("%26path%3D")
    if(idxPage < 0) ("","")
    else if(idxPath < 0) (ppStr.substring(idxPage+10),"")
    else (ppStr.substring(idxPage+10,idxPath),ppStr.substring(idxPath+10))
  }

  def keyProcess(key:String) = {
    if(key == "ProductModel") "productModel"
    else {
      regexWord findFirstIn key match {
        case Some(m) => key
        case None => "corruptKey"
      }
    }
  }

  def logProcess(log:String) = {
    val json = log2json(log)
    if(json != null){
      val logType = json.optString("logType")
      //  val productSN = json.optString(PRODUCT_SN)
      (logType,json.toString)
    }else null
  }

  def logProcessForActivity(log:String) = {
    val json = log2json(log)
    if(json != null){
     json.toString
    }else null
  }

  def logProcessMedusa(log:String) = {
    val json = if(log != null && !log.contains("jsonlog=")) log2json(log) else null
    if(json != null){
      val logType = json.optString("logType")
      val userId = json.optString("userId")
      (logType,userId,json.toString)
    }else null
  }

  def logProcessLogin(log:String) = {
    val json = loginlog2json(log)
    if(json != null){
      json.put(LOG_TYPE,"loginlog")
      val pc = json.optString("promotionChannel")
      val version = json.optString("version")
      if(PromotionChannelListUtil.isWhite(pc) && ApkVersionListUtil.isWhite(version)) json.toString else null
    }else null
  }

  def logProcessMetis(log:String) = {
    val json = log2json(log)
    if(json != null){
      val logType = json.optString("logType")
      (logType,json.toString)
    }else null
  }
}
