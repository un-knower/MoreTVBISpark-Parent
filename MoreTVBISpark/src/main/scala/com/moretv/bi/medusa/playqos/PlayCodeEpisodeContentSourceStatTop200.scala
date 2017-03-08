package com.moretv.bi.medusa.playqos

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, ProgramRedisUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 3/8/17.
  */
object PlayCodeEpisodeContentSourceStatTop200 extends BaseClass {
  private val tableName = "medusa_video_content_type_playqos_playcode_source_top200"
  private val arr = Array("movie", "mv", "sports", "tv", "hot", "zongyi", "comic", "xiqu", "jilu", "kids")
  private val limit = 200

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val tmpSqlContext = sqlContext
        import tmpSqlContext.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        var readPath = ""
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MERGER, LogTypes.PLAYVIEW, date).
            select("userId", "contentType", "event", "episodeSid")
            .registerTempTable("log_data")
          sqlContext.sql("select contentType,episodeSid,count(userId) as playNum" +
            " from log_data where event in ('startplay','playview') and contentType in ('sports','mv','movie','tv','hot','zongyi'," +
            "'comic','xiqu','jilu','kids') group by contentType,episodeSid").toDF("contentType", "episodeSid", "playNum").
            registerTempTable("log_play_num")

          if (date.equals("20160815")) {
            readPath = s"/log/medusa/parquet/20160814/playqos"
            cal.add(Calendar.DAY_OF_MONTH, -1)
          }
          else {
            readPath = s"/log/medusa/parquet/$date/playqos"
          }
          val rdd = sqlContext.read.parquet(readPath).select("userId", "date", "jsonLog")
            .map(e => (e.getString(0), e.getString(1), e.getString(2))).filter(_._2 == insertDate)
          rdd.flatMap(e => getPlayCode(e._1, e._2, e._3)).toDF("userId", "episodeSid", "day", "source", "playCode", "contentType").
            registerTempTable("log_playqos")


          if (p.deleteOld) {
            val deleteSql = s"delete from $tableName where day = ?"
            util.delete(deleteSql, insertDate)
          }
          val insertSql = s"insert into $tableName(day,episodeSid,title,source,contentType,playcode,num,sourceNum) values(?,?,?,?,?,?,?,?)"

          // Getting the playqos info

          arr.foreach(contentType => {
            sqlContext.sql(
              s"""
                 |select distinct contentType,episodeSid,playNum
                 |from log_play_num
                 |where contentType = '${contentType}'
                 |order by playNum desc
                 |limit ${limit}
              """.stripMargin).registerTempTable("log_episodesid")

            val numRdd = sqlContext.sql(
              """
                |select a.contentType,a.episodeSid,a.source,a.playCode,count(a.userId)
                |from log_playqos as a
                |join log_episodesid as b
                |on a.contentType = b.contentType and a.episodeSid = b.episodeSid
                |group by a.contentType,a.episodeSid,a.source,a.playCode
              """.stripMargin).map(e => ((e.getString(0), e.getString(1), e.getString(2)), (e.getInt(3), e.getLong(4))))

            val sourceRdd = sqlContext.sql(
              """
                |select a.contentType,a.episodeSid,a.source,count(a.userId)
                |from log_playqos as a
                |join log_episodesid as b
                |on a.contentType = b.contentType and a.episodeSid = b.episodeSid
                |group by a.contentType,a.episodeSid,a.sourceR
              """.stripMargin).map(e => ((e.getString(0), e.getString(1), e.getString(2)), e.getLong(3)))

            numRdd.join(sourceRdd).foreachPartition(partition => {
              val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
              partition.foreach(rdd => {
                util.insert(insertSql, insertDate, rdd._1._2, ProgramRedisUtil.getTitleBySid(rdd._1._2), rdd._1._3,
                  rdd._1._1, rdd._2._1._1, rdd._2._1._2, rdd._2._2)
              })
            })
          })

          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }

  }

  def getPlayCode(userId: String, day: String, str: String) = {

    val res = new ListBuffer[(String, String, String, String, Int, String)]()

    try {
      val jsObj = new JSONObject(str)

      val episodeSid = jsObj.optString("episodeSid")
      val contentType = jsObj.optString("contentType")
      val playqosArr = jsObj.optJSONArray("playqos")

      if (playqosArr != null) {

        (0 until playqosArr.length).foreach(i => {
          val playqos = playqosArr.optJSONObject(i)
          val source = playqos.optString("videoSource")
          val sourcecases = playqos.optJSONArray("sourcecases")

          if (sourcecases != null) {
            (0 until sourcecases.length).foreach(w => {
              val sourcecase = sourcecases.optJSONObject(w)
              res.+=((userId, episodeSid, day, source, groupCode(sourcecase.optInt("playCode")), contentType))
            })
          }
        })
      }
    }
    catch {
      case ex: Exception => {
        res.+=((userId, "", day, "", 0, ""))
        //throw ex
      }
    }
    res.toList
  }

  def groupCode(i: Int): Int = {
    i match {
      case -1 => -1
      case -2 => -2
      case _ => i
    }
  }
}
