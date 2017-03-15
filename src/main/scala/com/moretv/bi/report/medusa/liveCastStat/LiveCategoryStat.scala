package com.moretv.bi.report.medusa.liveCastStat

import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.LogType
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 1/11/17.
  */
object LiveCategoryStat extends BaseClass {

  private val tableName = "live_day_category_stat"

  private val fields = "day,category,play_num,play_user,view_num,view_user,play_duration"

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

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val categoryDF = LiveOneLevelCategory.code2Name("webcast", sc)

        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)

          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)


          val df = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogType.LIVE, loadDate)
            .filter($"liveType" === "live" && $"date" === sqlDate && $"pathMain".isNotNull)
          val playDf = df
            .join(categoryDF, df("liveMenuCode") === categoryDF("liveMenuCode"))
            .withColumnRenamed("name", "category")

          //          val viewDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogType.TABVIEW, loadDate)
          //            .filter($"stationcode".isin(LiveSationTree.Live_First_Category: _*)
          //              && $"date" === sqlDate)

          //TODO 此处未过滤类型
          val viewDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogType.TABVIEW, loadDate)
            .filter($"date" === sqlDate)

          val playDataset = playDf.filter($"event" === "startplay")
            .groupBy($"category")
            .agg(count($"userId").as("play_num"), countDistinct($"userId").as("play_user"))
            .as("t1")
            .join(
              playDf.filter($"event" === "switchchannel" && $"date" === sqlDate && $"duration".between(1, 36000))
                .groupBy($"category")
                .agg(sum($"duration").as("duration"))
                .as("t2"),
              $"t1.category" === $"t2.category"
            )
            .select($"t1.category", $"t2.duration", $"t1.play_num", $"t1.play_user")

          val viewDataset = viewDf.groupBy($"stationcode".as("category"))
            .agg(
              countDistinct($"userId").as("view_user"),
              count($"userId").as("view_num")
            )

          System.out.println("before delete, p.deleteOld is " + p.deleteOld)

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          playDataset.as("p").join(viewDataset.as("v"), $"p.category" === $"v.category")
            .select($"p.category", $"p.play_num", $"p.play_user", $"v.view_num", $"v.view_user", $"p.duration")
            .collect
            .foreach(e => {
              println(s"""${e.getString(0)}->${e.getLong(1)}->${e.getLong(2)}""")
              println("==============")

              util.insert(
                insertSql, sqlDate, e.getString(0), e.getLong(1), e.getLong(2), e.getLong(3), e.getLong(4), e.getLong(5)
              )
            })

        })

      }
      case None => {

      }
    }
  }


}
