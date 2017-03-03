package com.moretv.bi.report.medusa.liveCastStat

import java.sql.Timestamp
import java.util
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
import org.apache.spark.storage.StorageLevel


/**
  * Created by witnes on 2/14/17.
  */
object LiveChannelPlayStat extends BaseClass {


  import com.moretv.bi.report.medusa.liveCastStat.DimForLive._

  import com.moretv.bi.report.medusa.liveCastStat.FuncForLive._


  private val liveChannelPlayDayTable = "live_channel_day_play_stat"

  private val insertSqlForDayTable =
    s"insert into $liveChannelPlayDayTable(${cube4DFields.mkString(",")}) values(${List.fill(cube4DFields.length)("?").mkString(",")})"


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

        val categoryDF = LiveOneLevelCategory.code2Name("webcast", sc).persist(StorageLevel.MEMORY_AND_DISK)

        val categoreyTelecastDF = LiveOneLevelCategory.code2Name("telecast", sc).persist(StorageLevel.MEMORY_AND_DISK)


        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          if (p.deleteOld) {
            val url1 = s"http://${Constants.ES_URL}/medusa/channelMinutePlay/_query?q=date:${sqlDate}"
            val url2 = s"http://${Constants.ES_URL}/medusa/channel10MinutePlay/_query?q=date:${sqlDate}"
            val url3 = s"http://${Constants.ES_URL}/medusa/channelSidLiveInfo/_query?q=date:${sqlDate}"

            HttpUtils.delete(url1)
            HttpUtils.delete(url2)
            HttpUtils.delete(url3)
          }


          // 直播播放量
          val playDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogType.LIVE, loadDate)
            .filter($"liveType" === "live" && $"date" === sqlDate).persist(StorageLevel.MEMORY_AND_DISK_2)



            // 统计全网直播和电台直播每个节目的总播放情况
          val df1 = playDf.filter($"event" === "startplay")
            .groupBy(groupFields4D.map(w => col(w)): _*)
            .agg(count($"userId").as(VV), countDistinct($"userId").as(UV))
            .join(
              playDf.filter($"event" === "switchchannel" && $"duration".between(10, 36000))
                .groupBy(groupFields4D.map(w => col(w)): _*)
                .agg(sum($"duration").as(DURATION)),
              groupFields4D
            )

//          全网直播
            df1
            .join(categoryDF, df1(LIVECATEGORYCODE) === categoryDF(CATEGORYCODE), "inner").
              withColumnRenamed("code","liveMenuName")
            .foreachPartition(partition=>{
              val liveSidPlayList = new ArrayList[Map[String,Object]]()
              partition.foreach(w => {
                liveSidPlayList.add(w.getValuesMap(cube4DFields))
              })
              ElasticSearchUtil.bulkCreateIndex1(liveSidPlayList,"medusa","channelSidLiveInfo")
            })
//          电台直播
          df1
            .join(categoreyTelecastDF, df1(LIVECATEGORYCODE) === categoreyTelecastDF(CATEGORYCODE), "inner").
            withColumnRenamed("code","liveMenuName")
            .foreachPartition(partition=>{
              val liveSidPlayList = new ArrayList[Map[String,Object]]()
              partition.foreach(w => {
                liveSidPlayList.add(w.getValuesMap(cube4DFields))
              })
              ElasticSearchUtil.bulkCreateIndex1(liveSidPlayList,"medusa","channelSidLiveInfo")
            })


          //统计电台直播的自定义查询
          val df2 = playDf
            .filter($"event" === "startplay")
            .withColumn(HOUR, hour($"datetime")).withColumn(MINUTE, minute($"datetime"))
            .groupBy(groupFields4M.map(w => col(w)): _*)
            .agg(count($"userId").as(VV))

            df2.join(categoreyTelecastDF, df2(LIVECATEGORYCODE) === categoreyTelecastDF(CATEGORYCODE), "inner")
            .repartition(3).foreachPartition(partition=>{
              val channelMPlayList = new ArrayList[Map[String, Object]]()
              partition.foreach(w => {
                channelMPlayList.add(w.getValuesMap(cube4MFieldsV))
              })
              ElasticSearchUtil.bulkCreateIndex1(channelMPlayList, "medusa", "channelMinutePlay")
            })



          // 统计电台直播和全网直播每隔10分钟的在线人数
          val df3 = playDf.filter($"event" === "switchchannel" && $"duration".between(10, 36000))
            .withColumn("period",
              periodFillingWithStartEndUdf(
                hour((unix_timestamp($"datetime") - $"duration").cast("timestamp")),
                hour((unix_timestamp($"datetime").cast("timestamp"))),
                minute((unix_timestamp($"datetime") - $"duration").cast("timestamp")),
                minute(unix_timestamp($"datetime").cast("timestamp"))
              )
            )
            .withColumn("period", explode($"period"))
            .withColumn(MINUTE, $"period._2")
            .withColumn(HOUR, $"period._1")
                      .groupBy(groupFields4M.map(w => col(w)): _*)
            .agg(countDistinct($"userId").as(UV))
//          全网直播
            df3
            .join(categoryDF, df3(LIVECATEGORYCODE) === categoryDF(CATEGORYCODE), "inner")
            .repartition(3).foreachPartition(partition=>{
              val channel10MPlayList = new ArrayList[Map[String, Object]]()
              partition.foreach(w => {
                channel10MPlayList.add(w.getValuesMap(cube4MFieldsU))
              })
              ElasticSearchUtil.bulkCreateIndex1(channel10MPlayList, "medusa", "channel10MinutePlay")
            })
//          电台直播
          df3
            .join(categoreyTelecastDF, df3(LIVECATEGORYCODE) === categoreyTelecastDF(CATEGORYCODE), "inner")
            .repartition(3).foreachPartition(partition=>{
            val channel10MPlayList = new ArrayList[Map[String, Object]]()
            partition.foreach(w => {
              channel10MPlayList.add(w.getValuesMap(cube4MFieldsU))
            })
            ElasticSearchUtil.bulkCreateIndex1(channel10MPlayList, "medusa", "channel10MinutePlay")
          })


          categoryDF.unpersist()
          categoreyTelecastDF.unpersist()
          playDf.unpersist()
        })

      }
      case None => {

      }
    }
  }


}
