package com.moretv.bi.medusa.liveqos

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.temp.annual.PlayContentTypeStat._
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.json.JSONArray

import scala.collection.mutable.ArrayBuffer

object LiveQosStat extends BaseClass {

  private val tableName = "live_channel_source_code_day_stat"

  private val fields = "day,channelSid,channelName,sourceName,playCode,vv,uv"

  private val insertSql = s"insert into $tableName($fields)values(?,?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val q = sqlContext
        import q.implicits._

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(e => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          util.delete(deleteSql, sqlDate)

          val df = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.LIVEQOS, loadDate)
            .filter($"date" === sqlDate)
            .select($"userId", json_tuple($"jsonLog", "liveSid", "liveName", "liveqos"))
            .toDF("userId", "liveSid", "liveName", "liveqos")
            .filter($"liveqos".isNotNull)
            .select($"*", explode(getSourcePlayStat($"liveqos")).as("sourcestat"))
            .drop($"liveqos")
            .select($"*", $"sourcestat.*")
            .toDF("userId", "liveSid", "liveName", "sourceStat", "sourceName", "playCode")
            .drop($"sourceStat")

          df.groupBy($"liveSid", $"liveName", $"sourceName", $"playCode")
            .agg(count($"userId"), countDistinct($"userId"))
            .collect.foreach(w => {
            util.insert(insertSql, sqlDate, w.getString(0), w.getString(1), w.getString(2), w.getInt(3),
              w.getLong(4), w.getLong(5))
          })

        })


      }
      case None => {

      }
    }
  }

  val getSourcePlayStat = udf((s: String) => {
    val jsonArr = new JSONArray(s)
    var arrBuffer = new ArrayBuffer[(String, Int)]

    (0 until jsonArr.length).foreach(idx => {

      val js = jsonArr.optJSONObject(idx)
      val sourceName = js.optString("videoSource")
      val sourceArr = js.optJSONArray("sourcecases")
      if(sourceArr!=null){
        (0 until sourceArr.length).foreach(dx => {
          val code = sourceArr.optJSONObject(dx).getInt("playCode")
          arrBuffer.append((sourceName, code))
        })
      }
    })

    arrBuffer.toArray
  })

}
