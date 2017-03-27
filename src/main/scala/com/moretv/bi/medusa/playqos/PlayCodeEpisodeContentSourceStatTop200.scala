package com.moretv.bi.medusa.playqos

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, ProgramRedisUtil}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 3/8/17.
  */
object PlayCodeEpisodeContentSourceStatTop200 extends BaseClass {

  private val tableName = "medusa_episode_content_type_playqos_playcode_source_top200"

  val insertSql = s"insert into $tableName(day,episodeSid,title,source,contentType,playcode,num,sourceNum) values(?,?,?,?,?,?,?,?)"


  private val limit = 200

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val sqlContext = new HiveContext(sc)

        val q = sqlContext

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val getPlayCodeUdf = udf[List[(String, String, Int)], String](getPlayCode)
        val getTitleUdf = udf[String, String](ProgramRedisUtil.getTitleBySid)

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {

          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)

          if (p.deleteOld) {
            val deleteSql = s"delete from $tableName where day = ?"
            util.delete(deleteSql, insertDate)
          }

          val episodePlayDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MERGER, LogTypes.PLAYVIEW, date)
            .filter($"event" === "startplay" && $"date" === insertDate)
            .select($"userId", $"contentType", $"episodeSid")


          val w = Window.partitionBy($"contentType").orderBy($"vv".desc)

          episodePlayDf
            .groupBy($"contentType", $"episodeSid")
            .agg(count($"userId").as("vv"))
            .withColumn("rank", rank().over(w))
            .filter($"rank" <= 200)


          val playqosDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MERGER, LogTypes.PLAYQOS, date)
            .select($"userId", explode(getPlayCodeUdf($"jsonLog").as("tuple")))
            .withColumn("episodeSid", $"tuple._1")
            .withColumn("source", $"tuple._2")
            .withColumn("playcode", $"tuple._3")
            .withColumn("title", getTitleUdf($"episodeSid"))

          val episodeSourceDf = playqosDf.groupBy($"episodeSid", $"source")
            .agg(count($"userId").as("num"))

          val episodePlayCodeDf = playqosDf.groupBy($"episodeSid", $"source", $"playcode")
            .agg(count($"userId").as("sourceNum"))

          episodeSourceDf.join(episodePlayCodeDf, "episodeSid" :: Nil)
            .join(playqosDf, "episodeSid" :: Nil)
            .show(100, false)


          //            .collect.foreach(w => {
          //
          //            util.insert(insertSql, insertDate, w.getString(1), w.getString(4), w.getString(2),)
          //          })
        })


        cal.add(Calendar.DAY_OF_MONTH, -1)
      }

      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }


    def getPlayCode(jsonStr: String) = {

      val res = new ListBuffer[(String, String, Int)]()

      try {
        val jsObj = new JSONObject(jsonStr)

        val episodeSid = jsObj.optString("episodeSid")
        val playqosArr = jsObj.optJSONArray("playqos")

        if (playqosArr != null) {

          (0 until playqosArr.length).foreach(i => {
            val playqos = playqosArr.optJSONObject(i)
            val source = playqos.optString("videoSource")
            val sourcecases = playqos.optJSONArray("sourcecases")

            if (sourcecases != null) {
              (0 until sourcecases.length).foreach(w => {
                val sourcecase = sourcecases.optJSONObject(w)
                res.+=((episodeSid, source, sourcecase.optInt("playCode")))
              })
            }
          })
        }
      }
      catch {
        case ex: Exception => {
          res.+=(("", "", 0))
          //throw ex
        }
      }
      res.toList
    }


  }
}
