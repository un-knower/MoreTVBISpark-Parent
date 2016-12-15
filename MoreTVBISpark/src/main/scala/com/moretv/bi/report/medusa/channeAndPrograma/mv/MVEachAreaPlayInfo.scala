package com.moretv.bi.report.medusa.channeAndPrograma.mv

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by xiajun on 2016/5/16.
 * 推荐位节目、精选集播放情况
 */
object MVEachAreaPlayInfo extends BaseClass{
  def main(args: Array[String]): Unit = {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(MVEachAreaPlayInfo,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val s = sqlContext
        import s.implicits._
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

          sqlContext.read.parquet(playviewInput).select("userId","event","pathMain","contentType","duration")
            .registerTempTable("log_data")

          sqlContext.sql("select case when pathMain like '%mv_collection' then 'mine' when pathMain like '%mv_station'" +
            " then 'station' when pathMain like '%mvRecommendHomePage%' and pathMain not like '%mv_station%' then 'recommendation' when " +
            "pathMain like '%mvTopHomePage%' then 'top' when pathMain like '%mvCategoryHomePage%' then 'classification' when pathMain" +
            " like '%mv*function%' or pathMain like '%mv-search%' then 'entrance' else 'other' end, case when pathMain like " +
            "'%mv_collection' then 'All' when pathMain like '%mv_station' then 'All' when pathMain like '%mvRecommendHomePage%' " +
            "and pathMain not like '%mv_station%' then 'All' when pathMain like '%mvTopHomePage%' then 'All' when pathMain like " +
            "'%site_mvstyle%' then 'style' when pathMain like '%site_mvarea%' then 'area' when pathMain like '%site_mvyear%' then 'year' when pathMain " +
            "like '%mv-search%' then 'search' when pathMain like '%mv*function*site_hotsinger%' then 'singer' when " +
            "pathMain like '%mv*function*site_mvsubject%' then 'mvsubject' when pathMain like '%mv*function*site_concert%' then 'concert' else 'other' end, " +
            "userId,event,case when duration is null then 0 else duration end,contentType from log_data")

                .map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getLong(4),e.getString(5)))
                 .toDF("area","location","userId","event","duration","contentType").registerTempTable("log")

          val playRdd = sqlContext.sql("select area,location,count(userId),count(distinct userId) " +
            "from log where event='startplay' and contentType='mv' group by area,location")
            .map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3)))
            .filter(_._1!="other")
            .collect()

          val durationRdd = sqlContext.sql("select area,location,sum(duration),count(distinct " +
            "userId) from log where event!='startplay' and contentType='mv' and duration between 0 and 10800 group by" +
            " area,location")
            .map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3)))
            .filter(_._1!="other")
            .collect()

          val insertSql = "insert into medusa_channel_mv_each_location_play_info(day,area,location,play_num," +
            "play_user) values(?,?,?,?,?)"

          val insertSql1 = "insert into medusa_channel_mv_each_location_play_duration_info(day,area,location," +
            "play_duration,play_user) values(?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql1="delete from medusa_channel_mv_each_location_play_info where day=?"
            val deleteSql2="delete from medusa_channel_mv_each_location_play_duration_info where day=?"
            util.delete(deleteSql1,insertDate)
            util.delete(deleteSql2,insertDate)
          }

          playRdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,e._2,new JLong(e._3),new JLong(e._4))
          })

          durationRdd.foreach(e=>{
            util.insert(insertSql1,insertDate,e._1,e._2,new JLong(e._3),new JLong(e._4))
          })
        })
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
