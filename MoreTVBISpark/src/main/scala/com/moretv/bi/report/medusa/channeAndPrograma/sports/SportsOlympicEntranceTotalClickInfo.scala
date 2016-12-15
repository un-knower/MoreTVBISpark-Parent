package com.moretv.bi.report.medusa.channeAndPrograma.sports

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/7/27.
 * 统计奥运直播中心播放人数、次数与时长
 *
 */
object SportsOlympicEntranceTotalClickInfo extends BaseClass{

  def main(args: Array[String]): Unit = {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(SportsOlympicEntranceTotalClickInfo,args)
  }

  def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaDir = "/log/medusa/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          val launcherDir = s"$medusaDir/$date/homeaccess/"
          val sportsDir = s"$medusaDir/$date/positionaccess"

          sqlContext.read.parquet(launcherDir).registerTempTable("log_data1")
          sqlContext.read.parquet(sportsDir).registerTempTable("log_data2")
          val medusaNum = sqlContext.sql("select count(userId) from log_data1 where accessArea='recommendation' and " +
            "accessLocation='olympic' and event='click'").map(e=>e.getLong(0)).collect()(0)
          val moretvNum = sqlContext.sql("select count(userId) from log_data2 where accessArea='League' and " +
            "accessLocation='olympic' and event='click'").map(e=>e.getLong(0)).collect()(0)
          val totalNum = medusaNum+moretvNum

          sqlContext.sql("select userId from log_data1 where accessArea='recommendation' and " +
            "accessLocation='olympic' and event='click' union select userId from log_data2 where accessArea='League' " +
            "and accessLocation='olympic' and event='click'").registerTempTable("log_data")

          val clickInfo = sqlContext.sql("select count(distinct userId) from log_data").map(e=>(e.getLong(0)))

          val insertSql = "insert into medusa_channel_sport_olympic_entrance_total_click_info(day,entrance,click_num," +
            "click_user) values (?,?,?,?)"

          if(p.deleteOld){
            val deleteSql = "delete from medusa_channel_sport_olympic_entrance_total_click_info where day=?"
            util.delete(deleteSql,insertDate)
          }
          clickInfo.collect.foreach(e=>{
             util.insert(insertSql,insertDate,"All",new JLong(totalNum),new JLong(e))
           })


        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

}
