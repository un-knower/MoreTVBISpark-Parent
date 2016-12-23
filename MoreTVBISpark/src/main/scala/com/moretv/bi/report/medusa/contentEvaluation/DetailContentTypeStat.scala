package com.moretv.bi.report.medusa.contentEvaluation

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.report.medusa.channeAndPrograma.mv.MVRecommendPlay._
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 10/12/16.
  */
object DetailContentTypeStat extends BaseClass {

  //private val dataSource = "detail"

  private val tableName = "detail_contenttype_stat"

  private val fields = "day, channel, pv, uv"

  private val insert1Sql = s"insert into $tableName ($fields) values(?,?,?,?)"

  private val delete1Sql = s"delete from $tableName  where day = ?"


  def main(args: Array[String]) {

    ModuleClass.executor(this,args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        // init
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        sqlContext.udf.register("en2Cn", en2Cn _)

        val startDate = p.startDate

        val cal = Calendar.getInstance

        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          /*val loadPath2 = s"/log/medusaAndMoretvMerger/$loadDate/$dataSource"


          val df2 = sqlContext.read.parquet(loadPath2)
            .select("userId", "contentType")
            .filter("contentType is not null")*/

          val df2=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MERGER,LogTypes.DETAIL,loadDate).select("userId", "contentType")
            .filter("contentType is not null")
          df2.registerTempTable("log_data")

          val dfPlay =
            df2.sqlContext.sql(
              "select en2Cn(contentType), count(userId) as pv, count(distinct userId) as uv from log_data" +
                " group by  en2Cn(contentType) "
            )

          if (p.deleteOld) {
            util.delete(delete1Sql, sqlDate)
          }

          dfPlay.collect.foreach(w => {
            val contentType = w.getString(0)
            val pv = w.getLong(1)
            val uv = w.getLong(2)
            util.insert(insert1Sql, sqlDate, contentType, new JLong(pv), new JLong(uv))
          })

        })
      }
      case None => {
        throw new Exception("ContentTypeStat fails")
      }
    }
  }

  def en2Cn(field: String) = {
    field match {
      case "mv" => "音乐"
      case "tv" => "电视"
      case "kids" => "少儿"
      case "comic" => "动漫"
      case "zongyi" => "综艺"
      case "sports" => "体育"
      case "jilu" => "纪实"
      case "xiqu" => "戏曲"
      case "hot" => "咨询短片"
      case "movie" => "电影"
      case _ => "其它"
    }
  }

}
