package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util
import java.util.Calendar

import cn.whaley.bi.utils.ElasticSearchUtil
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.elasticsearch.common.xcontent.XContentFactory._

/**
  * Created by xiajun on 2016/5/16.
  *
  */
object EachVideoPlayInfo2ES extends BaseClass {
  def main(args: Array[String]): Unit = {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(EachVideoPlayInfo2ES, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {

          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          val playviewInput = s"$medusaDir/$date/playview/"

          sqlContext.read.parquet(playviewInput)
            .filter("event is not null and contentType is not null and videoSid is not null and episodeSid is not null")
            .filter("event in ('startplay','playview')")
            .select("userId", "contentType", "videoSid", "episodeSid")
            .registerTempTable("log_data")

          val videoDf = sqlContext.sql(
            """
              |select contentType,videoSid,count(userId),count(distinct userId)
              | from log_data group by contentType,videoSid
            """.stripMargin)


          val episodeDf = sqlContext.sql(
            """
              |select contentType, episodeSid, count(userId), count(distinct userId)
              |  from log_data group by contentType, episodeSid
            """.stripMargin)

          val videoList = new util.ArrayList[util.Map[String, Object]]()

          val episodeList = new util.ArrayList[util.Map[String, Object]]()



          // 将数据插入ES
          videoDf.collect.foreach(e => {
            val resMap = new util.HashMap[String, Object]()
            resMap.put("contentType", e.getString(0))
            resMap.put("sid", e.getString(1))
            resMap.put("title", ProgramRedisUtil.getTitleBySid(e.getString(1)).toString)
            resMap.put("day", insertDate.toString)
            resMap.put("userNum", new JLong(e.getLong(2)))
            resMap.put("accessNum", new JLong(e.getLong(3)))
            videoList.add(resMap)
          })

          episodeDf.collect.foreach(e => {
            val resMap = new util.HashMap[String, Object]()
            resMap.put("contentType", e.getString(0))
            resMap.put("episodeSid", e.getString(1))
            resMap.put("title", ProgramRedisUtil.getTitleBySid(e.getString(1)).toString)
            resMap.put("day", insertDate.toString)
            resMap.put("userNum", new JLong(e.getLong(2)))
            resMap.put("accessNum", new JLong(e.getLong(3)))
            episodeList.add(resMap)
          })


          ElasticSearchUtil.bulkCreateIndex(videoList, "medusa", "programPlay")

          ElasticSearchUtil.bulkCreateIndex(episodeList, "medusa", "prograplay")
        })
        ElasticSearchUtil.close
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

}
