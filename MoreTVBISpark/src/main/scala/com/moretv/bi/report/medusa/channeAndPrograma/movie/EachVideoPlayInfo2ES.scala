package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util
import java.util.Calendar

import cn.whaley.bi.utils.{ElasticSearchUtil, HttpUtils}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Constants
import com.moretv.bi.global.LogTypes
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by xiajun on 2016/5/16.
  *
  */
object EachVideoPlayInfo2ES extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {

          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MERGER, LogTypes.PLAYVIEW, date)
            .filter("event is not null and contentType is not null and videoSid is not null and episodeSid is not null")
            .filter("event in ('startplay','playview')")
            .select("userId", "contentType", "videoSid", "episodeSid")
            .registerTempTable("log_data")

          val videoDf = sqlContext.sql(
            """
              |select contentType,videoSid,count(distinct userId),count(userId)
              | from log_data group by contentType,videoSid
            """.stripMargin)

          // 删除ES中旧数据
          if (p.deleteOld) {
            val url = s"http://${Constants.ES_URL}/medusa/programPlay/_query?q=day:${insertDate}"
            HttpUtils.delete(url)
          }


          // 将数据写入HDFS文件
          videoDf.map(e => (e.getString(0), e.getString(1), ProgramRedisUtil.getTitleBySid(e.getString(1)).toString,
            insertDate.toString, e.getLong(2), e.getLong(3))).repartition(1).saveAsTextFile(s"/log/medusa/es/${insertDate}/programPlay")

          // 将数据插入ES
          videoDf.foreachPartition(partititon => {
            val videoList = new util.ArrayList[util.Map[String, Object]]()
            partititon.foreach(e => {
              val resMap = new util.HashMap[String, Object]()
              resMap.put("contentType", e.getString(0))
              resMap.put("sid", e.getString(1))
              resMap.put("title", ProgramRedisUtil.getTitleBySid(e.getString(1)).toString)
              resMap.put("day", insertDate.toString)
              resMap.put("userNum", new JLong(e.getLong(2)))
              resMap.put("accessNum", new JLong(e.getLong(3)))
              videoList.add(resMap)
            })
            ElasticSearchUtil.bulkCreateIndex(videoList, "medusa", "programPlay")
          })
          ElasticSearchUtil.close
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

}
