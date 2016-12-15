package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, DBOperationUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject
/**
 * Created by Administrator on 2016/5/16.
 * 统计腾讯源视频播放长度大于30分钟的播放VV情况
 */
object PlayVVBySource extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=> {
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(dateTime)
          val medusaDir = s"/log/medusa/parquet/$dateTime/playqos"
          sqlContext.read.parquet(medusaDir).select("jsonLog").registerTempTable("log")
          val num = sqlContext.sql("select jsonLog from log where jsonLog is not null").map(e=>e.getString(0)).
            filter(e=>e.contains("\"videoSource\":\"qq\"")).map(e=>jsonLogParase(e)).filter(e=>{e>=1800 & e<=10800}).count()
          val insertSql = "insert into temp_for_shangzhaohui(day,num) values(?,?)"
          util.insert(insertSql,day,new JLong(num))
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None =>
    }
  }

  def jsonLogParase(jsonLog:String) ={
    var playDuration = 0L
    try{
      val jsonObj = new JSONObject(jsonLog)
      val playQosInfo = jsonObj.optJSONArray("playqos")
      var qqJsonObj = new JSONObject()
      (0 until playQosInfo.length()).foreach(i=>{
        val info = playQosInfo.getJSONObject(i)
        if(info.get("videoSource")=="qq"){
          qqJsonObj = info
        }
      })
      val sourceCasesInfo = qqJsonObj.optJSONArray("sourcecases")
      (0 until sourceCasesInfo.length()).foreach(j=>{
        val secondaryInfo = sourceCasesInfo.optJSONObject(j)
        if(secondaryInfo!=null){
          playDuration = playDuration + secondaryInfo.optLong("playDuration")
        }
      })
    }catch{
      case e:Exception =>
    }
    playDuration
  }
}
