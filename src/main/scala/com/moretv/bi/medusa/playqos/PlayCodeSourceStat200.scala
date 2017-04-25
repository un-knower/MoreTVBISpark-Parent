package com.moretv.bi.medusa.playqos

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, ProgramRedisUtil}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 3/8/17.
  */
object PlayCodeSourceStat200 extends BaseClass {


  private val tableName = "medusa_episode_content_type_playqos_playcode_source_top200"

  private val insertSql =
    s"insert into $tableName(day,episodeSid,title,source,contentType,playcode,num,sourceNum) values(?,?,?,?,?,?,?,?)"


  private val limit = 100

  def main(args: Array[String]): Unit = {

    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "80")

    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val sq = sqlContext
        import org.apache.spark.sql.functions._
        import sq.implicits._

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        var readPath = ""



        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)

          readPath = s"/log/medusa/parquet/$date/playqos"

          if (p.deleteOld) {
            val deleteSql = s"delete from $tableName where day = ?"
            util.delete(deleteSql, insertDate)
          }

          // play qos df
          val qosDf = sqlContext.read.parquet(readPath)
            .filter($"date" === insertDate)
            .select($"userId", $"date", $"jsonLog")
            .map(e => (e.getString(0), e.getString(1), e.getString(2))).filter(_._2 == insertDate)
            .flatMap(
              e => getPlayCode(e._1, e._2, e._3)
            )
            .toDF("userId", "episodeSid", "day", "source", "playCode", "contentType")
            .filter($"source".isin(getSites(): _*))
            .filter($"playCode" !== 200)
            .cache()


          getSites.foreach(site => {

            // top limit play info

            val codeAgg = qosDf.filter($"source" === site)
              .groupBy($"day", $"episodeSid", $"contentType", $"source", $"playCode")
              .agg(count($"userId").as("num"))


            val sourceAgg = qosDf.filter($"source" === site)
              .groupBy($"day", $"episodeSid", $"contentType", $"source")
              .agg(count($"userId").as("sourceNum"))
              .orderBy($"sourceNum".desc)
              .limit(100)

            codeAgg.as("c").join(sourceAgg.as("s"),
              $"c.day" === $"s.day"
                && $"c.episodeSid" === $"s.episodeSid"
                && $"c.contentType" === $"s.contentType"
                && $"c.source" === $"s.source")
              .select($"c.day", $"c.episodeSid", $"c.source", $"c.contentType", $"playCode", $"num", $"sourceNum")

              .collect.foreach(w => {
              util.insert(insertSql, w.getString(0), w.getString(1),
                ProgramRedisUtil.getTitleBySid(w.getString(1)), w.getString(2), w.getString(3), w.getInt(4),
                w.getLong(5), w.getLong(6))
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


  /**
    *
    * @param day
    * @param str json字符串
    * @return (userId, episodeSid, day, playcode)
    */
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

  //  def isContained(field:String):Boolean = {
  //    val splitArr = filterStr.split(",")
  //    splitArr.contains(field)
  //  }

  def groupCode(i: Int): Int = {
    i match {
      case -1 => -1
      case -2 => -2
      case _ => i
    }
  }


  def getSites(): Seq[String] = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    val sq = sqlContext
    import sq.implicits._

    val rdd = sc.textFile("/log/medusa/qos/live-site-info")
      .map(_.split("\t"))
      .map(attr => {
        Row(attr(0), attr(1), attr(2))
      })

    val fields = "admin,display,code".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    sqlContext.createDataFrame(rdd, schema)
      .select($"code")
      .collect().map(_.getString(0))
  }
}


