package com.moretv.bi.dbOperation.db


import java.util.Calendar

import cn.whaley.sdk.utils.DataFrameUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, HdfsUtil, ParamsParseUtil}

import scala.collection.mutable.ListBuffer
import org.apache.spark._


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
         // val inputDate = DateFormatUtils.readFormat.format(cal.getTime,-1)

          println(date)


          val sql = "select datetime,forwardedIp,jsonLog from log_data "
          val df =  DataFrameUtil.getDFByDateWithSql("medusa",date,"playqos",sql)
            .map(e=>(e.getString(0),e.getString(1),e.getString(2)))

           val jsonRdd =  df.map(e=>{
            try{
            val detailAction = ListBuffer[String]()


              val temp =  JSON.parseObject(e._3)
              val userId = temp.getString("userId")
              val aid = temp.getString("aid")
              val pathMain = temp.getString("pathMain")
              val pathSpecial = temp.getString("pathSpecial")
              val pathSub = temp.getString("pathSub")
              val accountId = temp.getString("accountId")
              val contentType = temp.getString("contentType")
              val  videoSid = temp.getString("videoSid")
              val episodeSid = temp.getString("episodeSid")
              val playType = temp.getString("playType")
              val initDuration = temp.getLong("initDuration")
              val getVideoInfoDuration = temp.getLong("getVideoInfoDuration")
              val playRate = temp.getDouble("playRate")
              val playStatus = temp.getLong("playStatus")
              val avgDownloadSpeed = temp.getLong("avgDownloadSpeed")
              val uploadTime = temp.getString("uploadTime")

              val playqosArr = temp.getJSONArray("playqos")

            for (i <- 0 until playqosArr.size()){
              val obj = playqosArr.getJSONObject(i)
              val videoSource = obj.getString("videoSource")
              val sourceIndex = obj.getString("sourceIndex")
              val sourceSwitch = obj.getString("sourceSwitch:auto")
              val tryList = obj.getJSONArray("sourcecases")
              for(i <- 0 until tryList.size() ){
                val tryObj = tryList.getJSONObject(i)
                val parseDuration = tryObj.getLong("parseDuration")
                val initBufferDuration = tryObj.getLong("initBufferDuration")
                val preAdDuration = tryObj.getLong("preAdDuration")
                val mediumAdDuration = tryObj.getLong("mediumAdDuration")
                val postAdDuration = tryObj.getLong("postAdDuration")
                val totalBufferTimes = tryObj.getLong("totalBufferTimes")
                val totalBufferDuration = tryObj.getLong("totalBufferDuration")
                val bufferDurations = tryObj.getJSONArray("bufferDurations")
                val playDuration = tryObj.getLong("playDuration")
                val playCode = tryObj.getString("playCode")
                val playerType = tryObj.getString("playerType")

               val result = new JSONObject()
                result.put("datetime",e._1)
                result.put("forwardedIp",e._2)
                result.put("userId",userId)
                result.put("aid",aid)
                result.put("pathMain",pathMain)
                result.put("pathSpecial",pathSpecial)
                result.put("pathSub",pathSub)
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
            detailAction.toList
            }catch {
              case e: Exception => null
            }
          }).filter(_ != null).flatMap(e => e)


          val outputPath = s"/log/medusa/parquet/$date/playQuality"

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

}
