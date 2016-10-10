package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}

import com.moretv.bi.report.medusa.util.DataFromDB
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel


/**
 * Created by Administrator on 2016/5/16.
 * 统计腾讯片源的播放量
 */
object TencentVideoPlayInfo2 extends SparkSetting{
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "15g").
      set("spark.executor.cores", "8").
      set("spark.cores.max", "150")
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)
    val util = new DBOperationUtils("medusa")
    import sqlContext.implicits._
    // play日志数据
    val dateTime = "20160{716,717,718,719,71*,72*,73*,80*,811,812,813,814,815,816}"
    val insertDate = "20160715~20160815"
    val medusaDir = s"/log/medusa/parquet/$dateTime/play"
    sqlContext.read.load(medusaDir).select("userId","contentType","videoSid","event","videoName").
      filter("event='startplay' and videoSid is not null").registerTempTable("log")
    val rdd = sqlContext.sql("select contentType,videoSid,count(userId) from log group by contentType,videoSid").
      map(e=>(e.getString(0),e.getString(1),e.getLong(2)))

    rdd.collect().foreach(r=>{
      util.insert("insert into baimao1(contentType,videoName,num) values(?,?,?)",r._1,
        ProgramRedisUtil.getTitleBySid(r._2),new JLong(r._3))
    })

  }

  def getTitle(sid:String) = {
    var title =""
    val titleMap:Map[String,String] = Map()
    if(titleMap!=null){
      title = titleMap.getOrElse(sid,"")
      if(title==""){
        title = ProgramRedisUtil.getTitleBySid(sid)
      }
    }
    title
  }
}
