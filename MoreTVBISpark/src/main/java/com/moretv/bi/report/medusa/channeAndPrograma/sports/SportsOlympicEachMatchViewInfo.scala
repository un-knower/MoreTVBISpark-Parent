package com.moretv.bi.report.medusa.channeAndPrograma.sports

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/7/27.
 * 统计奥运各个tab的播放人数、次数与时长
 *
 */
object SportsOlympicEachMatchViewInfo extends BaseClass{

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
      ModuleClass.executor(SportsOlympicEachMatchViewInfo,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        sqlContext.udf.register("getLeagueId",OlympicMatchUtils.getMatchLeague _)
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val medusaDir = "/log/medusa/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val playviewInput = s"$medusaDir/$date/matchdetail/"

          sqlContext.read.parquet(playviewInput).select("event","userId","matchSid","match").repartition(8).
            registerTempTable("log_data")

          val matchPlayInfoDF = sqlContext.sql("select matchSid,getLeagueId(matchSid),match,count(userId)," +
            "count(distinct userId) from log_data where event='view' and length(match)<100 group by matchSid,match," +
            "getLeagueId(matchSid)").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3),e.getLong(4))).
            filter(_._2=="241")

          val insertSql = "insert into medusa_channel_sport_olympic_each_match_view_info(day,matchSid,matchName,click_num," +
            "click_user) values(?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql = "delete from medusa_channel_sport_olympic_each_match_view_info where day = ?"
            util.delete(deleteSql,insertDate)
          }

          matchPlayInfoDF.collect().foreach(e=>{
            util.insert(insertSql,insertDate,e._1,e._3,new JLong(e._4), new JLong(e._5))
          })



        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

}
