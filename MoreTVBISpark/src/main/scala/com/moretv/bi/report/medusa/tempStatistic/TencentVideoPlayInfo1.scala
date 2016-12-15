package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}

import com.moretv.bi.report.medusa.util.DataFromDB
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 统计各个视频源的播放数据
 */
object TencentVideoPlayInfo1 extends SparkSetting{
  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)
    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
    import sqlContext.implicits._
    sqlContext.udf.register("getTitleBySid",DataFromRedisUtil.getProgramTitleBySid _)
    sqlContext.udf.register("getTencentCid2Sid", DataFromDB.getTencentCid2Sid _)
    // play日志数据
    val dateTime = "20160{716,717,718,719,71*,72*,73*,80*,811,812,813,814,815,816}"
    val insertDate = "20160715~20160815"
    val medusaDir = s"/log/medusa/parquet/$dateTime/play"
    sqlContext.read.load(medusaDir).registerTempTable("log")

    // 处理腾讯源数据
    sc.textFile("/xiajun/test/two.csv").toDF("cid").registerTempTable("log1")
    sqlContext.sql("select getTencentCid2Sid(cid) from log1 where getTencentCid2Sid(cid) is not null").
      map(e=>e.getString(0)).toDF("sid").registerTempTable("log2")
    sc.textFile("/xiajun/test/one.csv").toDF("title").registerTempTable("log3")
    val tencentVideo = sqlContext.sql("select title from log3").map(e=>e.getString(0)).collect()

    // 根据title来提取数据
    val playInfoByTitle = sqlContext.sql("select a.contentType,getTitleBySid(a.videoName),a.userId from log as a join log3 as b " +
      "on getTitleBySid(a.videoName) = b.title where event='startplay' and videoName is not null").map(e=>(e.getString(0),e.getString(1),e.getString(2)))
    // 根据sid来提取数据
    val playInfoBySid = sqlContext.sql("select contentType,getTitleBySid(a.videoName),a.userId from log as a join log2 as b on " +
      "a.videoSid = b.sid where event='startplay' and videoSid is not null").map(e=>(e.getString(0),e.getString(1),e.getString(2))).
      filter(e=>{!tencentVideo.contains(e._2)})
    // 合并数据
    val matchedVideoNum = playInfoByTitle.map(e=>e._2).distinct().count()
    println("The number of match is: ",matchedVideoNum)
    val playInfoRdd = (playInfoByTitle union playInfoBySid).map(e=>(e._1,e._3))

    val playNum = playInfoRdd.countByKey()
    val userNum = playInfoRdd.distinct().countByKey()

    val insertSql = "insert into baimao_plan(day,conetentType,play_num,play_user) values(?,?,?,?)"
    playNum.foreach(i=>{
      val user = userNum.get(i._1) match {
        case Some(p) => p
        case None => 0L
      }
      util.insert(insertSql,insertDate,i._1,new JLong(i._2),new JLong(user))
    })
  }


}
