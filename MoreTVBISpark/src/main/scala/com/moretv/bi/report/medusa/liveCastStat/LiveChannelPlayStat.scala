package com.moretv.bi.report.medusa.liveCastStat

import java.sql.Timestamp
import java.util.ArrayList
import java.util.Calendar

import scala.collection.immutable.List
import cn.whaley.bi.utils.{ElasticSearchUtil, HttpUtils}
import org.apache.spark.sql.functions._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.{Constants, LogType}
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by witnes on 2/14/17.
  */
object LiveChannelPlayStat extends BaseClass {


  import com.moretv.bi.report.medusa.liveCastStat.DimForLive._

  import com.moretv.bi.report.medusa.liveCastStat.FuncForLive._


  private val liveChannelPlayDayTable = "live_channel_day_play_stat"

  private val insertSqlForDayTable =
    s"insert into $liveChannelPlayDayTable(${groupFields4DChannelPlay.mkString(",")}) values(${List.fill(groupFields4DChannelPlay.length)("?").mkString(",")})"


  private val deleteSqlForDayTable = s"delete from $liveChannelPlayDayTable where $DAY = ?"


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val q = sqlContext

        import q.implicits._


        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        step = 10

        val periodFillingWithStartEndUdf = udf[Seq[(Int, Int)], Int, Int, Int, Int](periodFillingWithStartEnd)

        val categoryDF = LiveOneLevelCategory.code2Name("webcast", sc)


        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          if (p.deleteOld) {
            val url1 = s"http://${Constants.ES_URL}/medusa/channelMinutePlay/_query?q=day:${sqlDate}"
            val url2 = s"http://${Constants.ES_URL}/medusa/channel10MinutePlay/_query?q=day:${sqlDate}"

            HttpUtils.delete(url1)
            HttpUtils.delete(url2)
            util.delete(deleteSqlForDayTable, sqlDate)

          }

          val channelMPlayList = new ArrayList[Map[String, Object]]()

          val channel10MPlayList = new ArrayList[Map[String, Object]]()



          val playDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogType.LIVE, loadDate)
            .filter($"liveType" === "live" && $"date" === sqlDate)



          playDf.filter($"event" === "startplay")
            .groupBy(groupFields4D.map(w => col(w)): _*)
            .agg(count($"userId").as(VV), countDistinct($"userId").as(UV))
            .join(
              playDf.filter($"event" === "switchchannel" && $"duration".between(10, 36000))
                .groupBy(groupFields4D.map(w => col(w)): _*)
                .agg(sum($"duration").as(DURATION)),
              groupFields4D
            )
            .join(categoryDF, CATEGORYCODE :: Nil, "left_outer")
            .collect
            .foreach(w => {
              util.insert(
                insertSqlForDayTable, w.getValuesMap(cube4DFields).toArray: _*
              )
            })


          playDf
            .filter($"event" === "startplay")
            .withColumn(HOUR, hour($"datetime")).withColumn(MINUTE, minute($"datetime"))
            .groupBy(groupFields4M.map(w => col(w)): _*)
            .agg(count($"userId").as(VV))
            .join(categoryDF, CATEGORYCODE :: Nil, "left_outer")
            .collect
            .foreach(w => {
              channelMPlayList.add(w.getValuesMap(cube4MFieldsV))
            })




          playDf.filter($"event" === "switchchannel" && $"duration".between(10, 36000))
            .withColumn("period",
              periodFillingWithStartEndUdf(
                hour((unix_timestamp($"datetime") - $"duration").cast("timestamp")),
                hour((unix_timestamp($"datetime").cast("timestamp"))),
                minute((unix_timestamp($"datetime") - $"duration").cast("timestamp")),
                minute(unix_timestamp($"datetime").cast("timestamp"))
              )
            )
            .withColumn("period", explode($"period"))
            .withColumn(MINUTE, $"period._1")
            .withColumn(HOUR, $"period._2")
            .groupBy(groupFields4M.map(w => col(w)): _*)
            .agg(countDistinct($"userId").as(UV))
            .join(categoryDF, CATEGORYCODE :: Nil, "left_outer")
            .foreach(w => {
              channel10MPlayList.add(w.getValuesMap(cube4MFieldsU))
            })




          ElasticSearchUtil.bulkCreateIndex1(channelMPlayList, "medusa", "channelMinutePlay")
          ElasticSearchUtil.bulkCreateIndex1(channelMPlayList, "medusa", "channel10MinutePlay")

        })

      }
      case None => {

      }
    }
  }


}
