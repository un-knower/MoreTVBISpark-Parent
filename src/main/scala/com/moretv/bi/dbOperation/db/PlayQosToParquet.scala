package com.moretv.bi.dbOperation.db

import java.util.Calendar

import cn.whaley.sdk.utils.DataFrameUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, HdfsUtil, ParamsParseUtil}

import scala.collection.mutable.ListBuffer
import org.apache.spark._
import org.json.{JSONArray, JSONObject}


/**
  * Created by hh on 2017/9/21.
  * 将playqos的日志展开并作为新的parquet存储在hdfs上
  */
object PlayQosToParquet extends  BaseClass{

  private val outputPartitionNum = 40

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args :Array[String]) = {

    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(cal.getTime)


          val sql = "select datetime,forwardedIp,jsonLog from log_data "
          val df =  DataFrameUtil.getDFByDateWithSql("medusa",date,"playqos",sql)
            .map(e=>(e.getString(0),e.getString(1),e.getString(2)))

           val jsonRdd =  df.map(e=>{
            try{
            val detailAction = ListBuffer[String]()

              val temp =  new JSONObject(e._3)
              val userId = getStringFromJson(temp,"userId")
              val aid = getStringFromJson(temp,"aid")
              val pathMain = getStringFromJson(temp,"pathMain")
              val pathSpecial = getStringFromJson(temp,"pathSpecial")
              val pathSub =  getStringFromJson(temp,"pathSub")
              val accountId = getStringFromJson(temp,"accountId")
              val contentType = getStringFromJson(temp,"contentType")
              val  videoSid = getStringFromJson(temp,"videoSid")
              val episodeSid = getStringFromJson(temp,"episodeSid")
              val playType = getStringFromJson(temp,"playType")
              val initDuration = getLongFromJson(temp,"initDuration",-1L)
              val getVideoInfoDuration = getLongFromJson(temp,"getVideoInfoDuration",-1L)
              val playRate = getDoubleFromJson(temp,"playRate",-1.0)
              val playStatus = getLongFromJson(temp,"playStatus",-1L)
              val avgDownloadSpeed = getLongFromJson(temp,"avgDownloadSpeed",-1L)
              val uploadTime = getStringFromJson(temp,"uploadTime")


            //设置个字段的默认值，便于合并数据
            var playQos = "true"
            var videoSource = "none"
            var sourceIndex = -1L
            var sourceSwitch = "none"
            var sourceCases = "true"
            var parseDuration = -1L
            var initBufferDuration = -1L
            var preAdDuration = -1L
            var mediumAdDuration = -1L
            var postAdDuration = -1L
            var totalBufferTimes = -1L
            var totalBufferDuration = -1L
            var bufferDurations = new JSONArray
            var playDuration = -1L
            var playCode = "none"
            var playerType = "none"

            val playqosArr = temp.optJSONArray("playqos")

            if(playqosArr == null ) {
              playQos = "false"
              val result = new JSONObject()
              result.put("datetime",e._1)
              result.put("forwardedIp",e._2)
              result.put("userId",userId)
              result.put("aid",aid)
              result.put("pathMain",pathMain)
              result.put("pathSpecial",pathSpecial)
              result.put("pathSub",pathSub)
              result.put("playQos",playQos)
              result.put("accountId",accountId)
              result.put("contentType",contentType)
              result.put("videoSid",videoSid)
              result.put("episodeSid",episodeSid)
              result.put("playType",playType)
              result.put("initDuration",initDuration)
              result.put("getVideoInfoDuration",getVideoInfoDuration)
              result.put("playRate",playRate)
              result.put("playStatus",playStatus)
              result.put("avgDownloadSpeed",avgDownloadSpeed)
              result.put("uploadTime",uploadTime)
              result.put("videoSource",videoSource)
              result.put("sourceIndex",sourceIndex)
              result.put("sourceSwitch",sourceSwitch)
              result.put("parseDuration",parseDuration)
              result.put("initBufferDuration",initBufferDuration)
              result.put("preAdDuration",preAdDuration)
              result.put("mediumAdDuration",mediumAdDuration)
              result.put("postAdDuration",postAdDuration)
              result.put("totalBufferTimes",totalBufferTimes)
              result.put("totalBufferDuration",totalBufferDuration)
              result.put("bufferDurations",bufferDurations)
              result.put("playDuration",playDuration)
              result.put("playCode",playCode)
              result.put("playerType",playerType)
              detailAction += result.toString

            }else {
              for (i <- 0 until playqosArr.length()){
                val obj = playqosArr.optJSONObject(i)
                  videoSource = getStringFromJson(obj,"videoSource")
                  sourceIndex = getLongFromJson(obj,"sourceIndex",-1L)
                  sourceSwitch = getStringFromJson(obj,"sourceSwitch")
                  val tryList = obj.optJSONArray("sourcecases")
                  if(tryList == null){
                    sourceCases = "false"
                    val result = new JSONObject()
                    result.put("datetime",e._1)
                    result.put("forwardedIp",e._2)
                    result.put("userId",userId)
                    result.put("aid",aid)
                    result.put("pathMain",pathMain)
                    result.put("pathSpecial",pathSpecial)
                    result.put("pathSub",pathSub)
                    result.put("playQos",playQos)
                    result.put("accountId",accountId)
                    result.put("contentType",contentType)
                    result.put("videoSid",videoSid)
                    result.put("episodeSid",episodeSid)
                    result.put("playType",playType)
                    result.put("initDuration",initDuration)
                    result.put("getVideoInfoDuration",getVideoInfoDuration)
                    result.put("playRate",playRate)
                    result.put("playStatus",playStatus)
                    result.put("avgDownloadSpeed",avgDownloadSpeed)
                    result.put("uploadTime",uploadTime)
                    result.put("videoSource",videoSource)
                    result.put("sourceIndex",sourceIndex)
                    result.put("sourceSwitch",sourceSwitch)
                    result.put("parseDuration",parseDuration)
                    result.put("initBufferDuration",initBufferDuration)
                    result.put("preAdDuration",preAdDuration)
                    result.put("mediumAdDuration",mediumAdDuration)
                    result.put("postAdDuration",postAdDuration)
                    result.put("totalBufferTimes",totalBufferTimes)
                    result.put("totalBufferDuration",totalBufferDuration)
                    result.put("bufferDurations",bufferDurations)
                    result.put("playDuration",playDuration)
                    result.put("playCode",playCode)
                    result.put("playerType",playerType)
                    detailAction += result.toString

                  }else {
                    for(i <- 0 until tryList.length() ){
                      val tryObj = tryList.optJSONObject(i)
                        parseDuration = getLongFromJson(tryObj,"parseDuration",-1L)
                        initBufferDuration = getLongFromJson(tryObj,"initBufferDuration",-1L)
                        preAdDuration = getLongFromJson(tryObj,"preAdDuration",-1L)
                        mediumAdDuration = getLongFromJson(tryObj,"mediumAdDuration",-1L)
                        postAdDuration = getLongFromJson(tryObj,"postAdDuration",-1L)
                        totalBufferTimes = getLongFromJson(tryObj,"totalBufferTimes",-1L)
                        totalBufferDuration = getLongFromJson(tryObj,"totalBufferDuration",-1L)
                        bufferDurations = tryObj.optJSONArray("bufferDurations")
                        playDuration = getLongFromJson(tryObj,"playDuration",-1L)
                        playCode = getStringFromJson(tryObj,"playCode")
                        playerType = getStringFromJson(tryObj,"playerType")

                      val result = new JSONObject()
                      result.put("datetime",e._1)
                      result.put("forwardedIp",e._2)
                      result.put("userId",userId)
                      result.put("aid",aid)
                      result.put("pathMain",pathMain)
                      result.put("pathSpecial",pathSpecial)
                      result.put("pathSub",pathSub)
                      result.put("playQos",playQos)
                      result.put("accountId",accountId)
                      result.put("contentType",contentType)
                      result.put("videoSid",videoSid)
                      result.put("episodeSid",episodeSid)
                      result.put("playType",playType)
                      result.put("initDuration",initDuration)
                      result.put("getVideoInfoDuration",getVideoInfoDuration)
                      result.put("playRate",playRate)
                      result.put("playStatus",playStatus)
                      result.put("avgDownloadSpeed",avgDownloadSpeed)
                      result.put("uploadTime",uploadTime)
                      result.put("videoSource",videoSource)
                      result.put("sourceIndex",sourceIndex)
                      result.put("sourceSwitch",sourceSwitch)
                      result.put("parseDuration",parseDuration)
                      result.put("initBufferDuration",initBufferDuration)
                      result.put("preAdDuration",preAdDuration)
                      result.put("mediumAdDuration",mediumAdDuration)
                      result.put("postAdDuration",postAdDuration)
                      result.put("totalBufferTimes",totalBufferTimes)
                      result.put("totalBufferDuration",totalBufferDuration)
                      result.put("bufferDurations",bufferDurations)
                      result.put("playDuration",playDuration)
                      result.put("playCode",playCode)
                      result.put("playerType",playerType)
                      detailAction += result.toString

                    }
                  }
              }
            }
            detailAction.toList
            }catch {
              case e: Exception => null
            }
          }).filter(_ != null).flatMap(e => e)


          val outputPath = s"/log/medusa/parquet/$date/playqos_flatten"

          if (p.deleteOld) HdfsUtil.deleteHDFSFile(outputPath)
          sqlContext.read.json(jsonRdd).coalesce(outputPartitionNum).write.parquet(outputPath)

          cal.add(Calendar.DAY_OF_MONTH, -1)


        })

      }
      case  None =>{
        throw  new RuntimeException("At least need parm --startDate")
      }
    }

  }


  def getLongFromJson (tmp:JSONObject,key:String,init:Long):Long ={

  try{
   val value = tmp.get(key).toString
       value.toLong
  }catch{
    case e: Exception => init
  }

  }

  def getDoubleFromJson (tmp:JSONObject,key:String,init:Double):Double ={

    try{
      val value = tmp.get(key).toString
      value.toDouble
    }catch{
      case e: Exception => init
    }

  }


  def getStringFromJson (tmp:JSONObject,key:String):String ={

    try{
      val value = tmp.get(key).toString
      value
    }catch{
      case e: Exception => "none"
    }

  }


}
