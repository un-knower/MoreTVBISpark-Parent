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
object TencentVideoPlayInfo3 extends SparkSetting{
  val titleMap:Map[String,String] = Map()
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "15g").
      set("spark.executor.cores", "8").
      set("spark.cores.max", "150")
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)
    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
    import sqlContext.implicits._
    // play日志数据
    val dateTime = "20160{716,717,718,719,71*,72*,73*,80*,811,812,813,814,815,816}"
    val insertDate = "20160715~20160815"
    val medusaDir = s"/log/medusa/parquet/$dateTime/play"
    sqlContext.read.load(medusaDir).select("userId","contentType","videoSid","event","videoName").
      filter("event='startplay' and videoSid is not null").filter("contentType in ('mv','movie','tv'," +
      "'comic','jilu','zongyi','hot','kids','xiqu')").selectExpr("userId","videoSid",
        "contentType","videoName").registerTempTable("log")
    sqlContext.sql("select contentType,videoSid,count(userId) from log group by contentType," +
      "videoSid").map(e=>(e.getString(0),e.getString(1),
      ProgramRedisUtil.getTitleBySid(e.getString(1)),e.getLong(2))).
      toDF("contentType","videoSid","title","play_num").registerTempTable("log1")
    // 处理腾讯源数据
    sc.textFile("/xiajun/test/two.csv").map(e=>DataFromDB.getTencentCid2Sid(e)).toDF("sid").
      registerTempTable("log2")
    sc.textFile("/xiajun/test/one.csv").toDF("title").registerTempTable("log3")

    // 根据title来提取数据
    val playInfoByTitle = sqlContext.sql("select a.contentType,a.title,a.play_num " +
    "from log1 as a join log3 as b on a.title = b.title").
      map(e=>(e.getString(0),e.getString(1),e.getLong(2)))


    val insertSql = "insert into baimao2(conetentType,title,play_num) values(?,?,?)"
    playInfoByTitle.collect().foreach(r=>{
      util.insert(insertSql,r._1,r._2,new JLong(r._3))
    })
  }

  def getTitle(sid:String) = {
    var title =""
    if(titleMap!=null){
      title = titleMap.getOrElse(sid,"")
      if(title==""){
        title = ProgramRedisUtil.getTitleBySid(sid)
      }
    }
    title
  }
}
