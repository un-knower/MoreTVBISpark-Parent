package com.moretv.bi.util

import scala.collection.mutable


/**
 * Created by xiajun on 2016/7/29.
 * 记录medusa与moretv的parquet的所有字段信息
 */
object ParquetSchemaDimension {

  //delete weatherCode,uploadTime,groupId
  //add omnibusName,omnibusSid,topRankName,topRankSid,singerSid
  val schemaArr = Array("accessArea","accessLocation","accessSource","accountId","action","apkSeries","apkVersion",
    "appAnterWay","appEnterWay","appSid","belongTo","bufferTimes","buildDate","button","channelSid","collectClass",
    "collectContent","collectType","contentType","corruptKey","date","datetime","day","duration","entrance","entryWay",
    "episodeSid","episodyed_android_youku","event","happenTime","homeContent","homeLocation","homeType","ip",
    "jsonLog","liveName","liveSid","liveType","locationIndex","logType","logVersion","mark","match","matchSid",
    "newEntrance","oldEntrance","page","path","pathMain","pathSpecial","pathSub","productModel","programSid",
    "programType","promotionChannel","region","retrieval","searchText","singer","source","station","stationcode",
    "subjectCode","subscribeContent","subscribeType","switch","userId","versionCode","videoName",
    "videoSid","omnibusName","omnibusSid","topRankName","topRankSid","singerSid")

  // 处理parquet schema中类型

  def schemaTypeConvert(schemaString:Array[String]):Array[String] = {
    val newSchemaArr = mutable.ArrayBuffer[String]()
    schemaString.foreach(i=>{
      var e = i
      if(i=="duration") { e = "cast (duration as long) as duration"}
      else if(i=="groupId") { e = "cast (groupId as string) as groupId"}
      else if(i=="accountId") { e = "cast (accountId as long) as accountId"}
      newSchemaArr += e
    })
    newSchemaArr.toArray
  }

}
