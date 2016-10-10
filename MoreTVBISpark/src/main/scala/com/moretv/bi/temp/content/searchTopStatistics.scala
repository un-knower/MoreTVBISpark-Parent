package com.moretv.bi.temp.content

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.{ProgramRedisUtil, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by zhangyu on 16/8/17.
 * 统计通过搜索途径播放的某类节目的topN
 * table_name: search_top_statistics(day,content_type,videosid,content_name,access_num)
 */
object searchTopStatistics extends SparkSetting {
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        val cal1 = Calendar.getInstance()
        cal1.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val logDay = DateFormatUtils.readFormat.format(cal1.getTime)
          val sqlDay = DateFormatUtils.toDateCN(logDay, -1)
          val contentType = p.contentType
          val logPath = s"/log/medusaAndMoretvMerger/$logDay/playview"

          //println(contentType)

          sqlContext.read.load(logPath).registerTempTable("log_data")
          val rddSql = sqlContext.sql(s"select videoSid, count(userId) as access_num from log_data " +
            s"where contentType = '$contentType' " +
            s"and (path like '%search%' or pathMain like '%search%') " +
            s"and length(videoSid) = 12 " +
            s"group by videoSid " +
            s"order by access_num desc " +
            s"limit 20").map(row => {
            val contentTypeCh = fromEngToCh(contentType)
            val videoSid = row.getString(0)
            val contentName = ProgramRedisUtil.getTitleBySid(videoSid)
            val accessNum = row.getLong(1)
            //println(videoSid)
            (contentTypeCh,videoSid,contentName,accessNum)
          })

          if(p.deleteOld) {
            val deleteSql = "delete from search_top_statistics where day = ?"
            util.delete(deleteSql,sqlDay)
          }

          rddSql.collect().foreach(e=>{
            val insertSql = "insert into search_top_statistics(day,content_type,videosid,content_name,access_num) " +
              "values(?,?,?,?,?)"
            util.insert(insertSql,sqlDay,e._1,e._2,e._3,e._4)
          })

          cal1.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }

    }
  }


  def fromEngToCh(str:String):String = {
    if(str != null && str.nonEmpty){
      str match {
        case "jilu" => "纪实"
        case _ => "未知"
      }
    }else "未知"
  }


}
