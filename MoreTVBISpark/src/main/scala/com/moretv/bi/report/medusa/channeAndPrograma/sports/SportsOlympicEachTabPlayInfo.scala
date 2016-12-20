package com.moretv.bi.report.medusa.channeAndPrograma.sports

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/7/27.
 * 统计奥运各个tab的播放人数、次数与时长
 *
 */
object SportsOlympicEachTabPlayInfo extends BaseClass{

  def main(args: Array[String]): Unit = {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(SportsOlympicEachTabPlayInfo,args)
  }

  override def init() = {
    sc = new SparkContext(config)
    sqlContext = SQLContext.getOrCreate(sc)
    sqlContext.udf.register("getOlympicTabName",getOlympicTabName _)
  }

  override def execute(args: Array[String]) {
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

          val playviewInput = s"$medusaDir/$date/play/"

          sqlContext.read.parquet(playviewInput).select("userId","pathMain","event","duration")
            .registerTempTable("log_data")

          val playInfoDF = sqlContext.sql("select getOlympicTabName(pathMain),count(userId),count(distinct userId) from " +
            "log_data where event ='startplay' and pathMain like '%olympic%' and length(getOlympicTabName(pathMain))<20 " +
            " group by getOlympicTabName(pathMain)")
          val playDurationDF = sqlContext.sql("select getOlympicTabName(pathMain),sum(duration),count(distinct " +
            "userId) from log_data where event != 'startplay' and pathMain like '%olympic%' and " +
            "length(getOlympicTabName(pathMain))<20 and duration>=0 and duration<=21600 group by getOlympicTabName(pathMain)")

          val mergerRdd = playInfoDF.map(e=>(e.getString(0),(e.getLong(1),e.getLong(2)))).join(playDurationDF.map(e=>(e
            .getString(0),(e.getLong(1),e.getLong(2))))).filter(_._1.length<20)
          val insertSql = "insert into medusa_channel_sport_olympic_each_tab_play_info(day,area,tab_name,play_num," +
            "play_user,total_duration,total_dur_user) values (?,?,?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql = "delete from medusa_channel_sport_olympic_each_tab_play_info where day=?"
            util.delete(deleteSql,insertDate)
          }
           mergerRdd.collect.foreach(e=>{
             util.insert(insertSql,insertDate,"olympic",e._1,new JLong(e._2._1._1),new JLong(e._2._1._2),new JLong(e._2._2
               ._1),
               new JLong(e._2._2._2))
           })


        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

  def getOlympicTabName(path:String)={
    var result:String=null
    try{
      if(path.contains("home*classification")){
        result = path.split("-")(2).split("\\*")(1)
      } else if(path.contains("home*recommendation")) {
        result = path.split("-")(1).split("\\*")(1)
      }
    } catch {
      case e:Exception => e.printStackTrace()
    }
    result
  }

}
