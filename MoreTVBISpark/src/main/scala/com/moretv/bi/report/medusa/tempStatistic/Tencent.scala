package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.DataFromDB
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by Administrator on 2016/4/24.
 * 统计腾讯源视频的VV与UV
 */
object Tencent extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "6g").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        sqlContext.udf.register("getTencentCid2Sid", DataFromDB.getTencentCid2Sid _)
        import sqlContext.implicits._
        sc.textFile("/xiajun/test/one.csv").toDF("cid").registerTempTable("log2")
        sqlContext.sql("select getTencentCid2Sid(cid) from log2 where getTencentCid2Sid(cid) is not null").
          map(e=>e.getString(0)).toDF("sid").registerTempTable("log1")
//        val matchedNum = sqlContext.sql("select sid from log1").map(e=>e.getString(0)).take(10)
//        matchedNum.foreach(i=>{
//          println(i)
//        })
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val inputDay = "20160{716,717,718,719,71*,72*,73*,80*,811,812,813,814,815,816}"
          val insertDay = "20160715~20160815"
          val inputPath = s"/log/medusa/parquet/$inputDay/play"
          sqlContext.read.load(inputPath).select("contentType","event","userId","videoSid","videoName").registerTempTable("log")
          val playRdd = sqlContext.sql("select contentType,videoSid,videoName,count(userId),count(distinct userId) " +
            "from log as a join log1 as b on a.videoSid=b.sid " +
            "where a.event='startplay' and a.videoSid is not null group by contentType,videoSid,videoName").
            map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3),e.getLong(4)))
//          val playRdd = sqlContext.sql("select contentType,count(userId),count(distinct userId) " +
//            "from log as a join log1 as b on a.videoSid=b.sid " +
//            "where a.event='startplay' and a.videoSid is not null group by contentType").
//            map(e=>(e.getString(0),e.getLong(1),e.getLong(2)))
//          val insertSQL = "insert into temp_tencent(day,contentType,vv,uv) values(?,?,?,?)"
//          playRdd.collect().foreach(i=>{
//            util.insert(insertSQL,insertDay,i._1,new JLong(i._2),new JLong(i._3))
//          })
          val inserttSQL = "insert into temp_tencent1(day,contentType,videoSid,videoName,vv,uv) values(?,?,?,?,?,?)"
          playRdd.collect().foreach(i=>{
            util.insert(inserttSQL,insertDay,i._1,i._2,i._3,new JLong(i._4),new JLong(i._5))
          })
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }

  }
}
