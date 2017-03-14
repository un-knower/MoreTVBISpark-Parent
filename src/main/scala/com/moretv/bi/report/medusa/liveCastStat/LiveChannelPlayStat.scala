package com.moretv.bi.report.medusa.liveCastStat

import java.util.{ArrayList, Calendar}

import cn.whaley.bi.utils.ElasticSearchUtil
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.LogType
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConversions._

/**
  * Created by witnes on 2/14/17.
  */
object LiveChannelPlayStat extends BaseClass {


  import com.moretv.bi.report.medusa.liveCastStat.DimForLive._
  import com.moretv.bi.report.medusa.liveCastStat.FuncForLive._


  def main(args: Array[String]) {

    // driverTask(args)

    clusterTask(args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        val q = sqlContext

        import q.implicits._

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        val periodFillingWithStartEndUdf = udf[Seq[(Int, Int)], Int, Int, Int, Int](periodFillingWithStartEnd)

        val categoryDF = LiveOneLevelCategory.code2Name("webcast", sc).persist(StorageLevel.MEMORY_AND_DISK)


        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)


          // 直播播放量
          val playDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogType.LIVE, loadDate)
            .filter($"liveType" === "live" && $"date" === sqlDate)
            .cache()



          val liveSidPlayList = new ArrayList[Map[String, Object]]()
          // 统计全网直播和电台直播每个节目的总播放情况
          playDf.filter($"event" === "startplay")
            .groupBy(groupFields4D.map(w => col(w)): _*)
            .agg(count($"userId").as(VV), countDistinct($"userId").as(UV))
            .join(
              playDf.filter($"event" === "switchchannel" && $"duration".between(10, 36000))
                .groupBy(groupFields4D.map(w => col(w)): _*)
                .agg(sum($"duration").as(DURATION)),
              groupFields4D
            )
            .join(categoryDF, LIVECATEGORYCODE :: Nil, "leftouter")
            .collectAsList.foreach(w => {
                liveSidPlayList.add(w.getValuesMap(cube4DFieldsUVD))
            })
          ElasticSearchUtil.bulkCreateIndex1(liveSidPlayList, ES_INDEX, ES_TYPE_D)




          val channelMPlayList = new ArrayList[Map[String, Object]]()
          //统计电台直播的自定义查询
          playDf
            .filter($"event" === "startplay")
            .withColumn(HOUR, hour($"datetime")).withColumn(MINUTE, minute($"datetime"))
            .groupBy(groupFields4M.map(w => col(w)): _*)
            .agg(count($"userId").as(VV))

            .join(categoryDF, LIVECATEGORYCODE :: Nil, "leftouter")
            .collect.foreach(w => {
              channelMPlayList.add(w.getValuesMap(cube4MFieldsV))
          })
          ElasticSearchUtil.bulkCreateIndex1(channelMPlayList, ES_INDEX, ES_TYPE_M)




          val channel10MPlayList = new ArrayList[Map[String, Object]]()
          // 统计电台直播和全网直播每隔10分钟的在线人数
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
            .withColumn(MINUTE, $"period._2").withColumn(HOUR, $"period._1")

            .groupBy(groupFields4M.map(w => col(w)): _*)
            .agg(countDistinct($"userId").as(UV))

            .join(categoryDF, LIVECATEGORYCODE :: Nil, "leftouter")

            .collect.foreach(w => {
              channel10MPlayList.add(w.getValuesMap(cube4MFieldsU))
          })
          ElasticSearchUtil.bulkCreateIndex1(channel10MPlayList, ES_INDEX, ES_TYPE_10M)

          categoryDF.unpersist()
          playDf.unpersist()
        })
      }

      case None => {
        println("At least One parameter for this program.")
      }


    }
  }

  def driverTask(args: Array[String]) = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        val cal = Calendar.getInstance
        if (p.startDate == null) {
          cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        }

        if (p.deleteOld) {

          (0 until p.numOfDays).foreach(w => {

            cal.add(Calendar.DAY_OF_MONTH, -1)
            val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

            ElasticSearchUtil.bulkDeleteDocs(ES_INDEX, ES_TYPE_D, DAY, sqlDate)
            ElasticSearchUtil.bulkDeleteDocs(ES_INDEX, ES_TYPE_M, DAY, sqlDate)
            ElasticSearchUtil.bulkDeleteDocs(ES_INDEX, ES_TYPE_10M, DAY, sqlDate)

          })
        }

      }
      case None => {
        println("At least One parameter for this program.")
      }

    }
  }

  def clusterTask(args: Array[String]) = {
    ModuleClass.executor(this, args)

  }
}
