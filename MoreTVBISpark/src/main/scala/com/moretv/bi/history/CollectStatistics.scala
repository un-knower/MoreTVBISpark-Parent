package com.moretv.bi.history

import java.lang.{Long => JLong}
import java.util.Calendar
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

object CollectStatistics extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(CollectStatistics,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val pathDetail = "/mbi/parquet/detail/"+date+"/*"
          val pathPlayview = "/mbi/parquet/playview/"+date+"/*"

          val dfDetail = sqlContext.read.parquet(pathDetail).filter("path = 'home-history-collect'").select("userId","contentType").cache()
          val dfPlayview = sqlContext.read.parquet(pathPlayview).filter("path = 'home-history-collect'").select("userId","contentType").cache()
          val detail_pv = rowArray2Map(dfDetail.groupBy("contentType").count().collect())
          val detail_uv = rowArray2Map(dfDetail.distinct().groupBy("contentType").count().collect())
          val playview_vv = rowArray2Map(dfPlayview.groupBy("contentType").count().collect())
          val playview_uv = rowArray2Map(dfPlayview.distinct().groupBy("contentType").count().collect())
          val res_df = detail_pv.map(e => {
            val contentType = e._1
            val contentTypeCH = contentType2CH(contentType)
            if (contentTypeCH != "") {
              val uv = detail_uv(contentType)//.where(s"contentType = '$contentType'").first().getLong(1)
              val pv = e._2
              val userPlay_num =
                playview_uv.get(contentType) match {
                  case Some(t) => t
                  case None => 0
                }
              val vv =
                playview_vv.get(contentType) match {
                  case Some(t) => t
                  case None => 0
                }
              (contentTypeCH, uv, pv, userPlay_num, vv)
            }
            else null
          }
          ).filter(_!=null)
          if(p.deleteOld){
            val sqlDelete = "DELETE FROM CollectPageStatistics WHERE day = ?"
            util.delete(sqlDelete,day)
          }

          val sqlInsert = "INSERT INTO CollectPageStatistics(day, contentType, user_num, access_num, userPlay_num, play_num) VALUES(?,?,?,?,?,?)"

          res_df.foreach(x =>
            util.insert(sqlInsert, day, x._1, new JLong(x._2), new JLong(x._3), new JLong(x._4), new JLong(x._5))
          )
          dfDetail.unpersist()
          dfPlayview.unpersist()
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
        util.destory()
      }
      case None =>
        throw new RuntimeException("At least need param --startDate.")

    }
  }

  def contentType2CH(contentType:String) : String = {
    contentType match {
      case "movie" => "电影收藏"
      case "tv" => "电视剧收藏"
      case "zongyi" => "综艺收藏"
      case "comic" => "动漫收藏"
      case "kids" => "少儿收藏"
      case "jilu" => "纪实收藏"
      case _ => ""
    }
  }
  def rowArray2Map(array:Array[Row]) = {
    array.map(row => (row.getString(0),row.getLong(1))).toMap
  }
}
