package com.moretv.bi.report.medusa.functionalStatistic.searchInfo

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.sql.functions._


/**
  * Created by witnes on 9/18/16.
  */

/**
  * 各个入口搜索占比
  */

object SearchEntranceStat extends BaseClass {

  private val tableName = "search_entrance_dist"

  private val fields = "day,entrance,uv,pv"

  private val insertSql = s"insert into $tableName($fields)values(?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val sq = sqlContext
        import sq.implicits._

        val fromEn2Cn = udf((s: String) => {
          s match {
            case "comic" => "动漫"
            case "mv" => "音乐"
            case "hot" => "咨讯短片"
            case "zongyi" => "综艺"
            case "movie" => "电影"
            case "launcher" => "launcher"
            case "kid-knowledge" => "少儿-学知识"
            case "kids-songs" => "少儿-听儿歌"
            case "kids-cartoon" => "少儿-看动画"
            case "xiqu" => "戏曲"
            case "jilu" => "记录"
            case "thirdparty" => "第三方"
            case _ => "其它"

          }
        })

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(w => {

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          val df = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.SEARCHENTRANCE2, loadDate)
            .filter($"date" === sqlDate)
            .select($"entrance", $"userId")
            .groupBy($"entrance")
            .agg(count($"userId").as("pv"), countDistinct($"userId").as("uv"))
            .withColumn("entrance", fromEn2Cn($"entrance"))

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          df.collect.foreach(w => {
            util.insert(insertSql, sqlDate, w.getString(0), w.getLong(1), w.getLong(2))
          })

        })

      }
      case None => {
        throw new Exception("SearchEntranceStatistic fail ")
      }
    }
  }
}
