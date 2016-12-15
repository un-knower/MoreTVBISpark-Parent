package com.moretv.bi.report.medusa.research

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/5/16.
 * 统计每个用户播放每个节目的播放情况！
 * 保存到HDFS中
 */
object getEachUserVideoOrderedInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaDir = "/log/medusa/research/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val playviewInput = s"$medusaDir/$date/eachUserEachVideoPlayInfo/"

          sqlContext.read.parquet(playviewInput).select("playNumber")
            .registerTempTable("log_data")

          val searchRdd = sqlContext.sql("select playNumber from log_data order by playNumber desc").map(e=>e.getLong(0))

          if (p.deleteOld) {
            val deleteSql = "delete from temp_each_user_video_play_num where date=?"
            util.delete(deleteSql,insertDate)
          }

          val insertSql = "insert into temp_each_user_video_play_num(day,play_num) values (?,?)"

          searchRdd.collect().foreach(e=>{
            util.insert(insertSql,insertDate,new JLong(e))
          })

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
